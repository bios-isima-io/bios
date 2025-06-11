/*
 * Copyright (C) 2025 Isima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "csdk_unit_manager.h"
#include "py_common.h"
#include "tfoscsdk/directapi.h"
#include "tfoscsdk/version.h"

static CSdkUnitManager *unit_manager = nullptr;

/**
 * Callback method invoked by CSDK when more can be sent.
 */
static void AskIngestBulk(int64_t bulk_ctx_id, int from_index, int to_index, void *cb_args) {
  DEBUG_LOG("Enter AskIngestBulk -- from = %d, to = %d, ctx = %ld", from_index, to_index,
            bulk_ctx_id);

  auto *context = reinterpret_cast<CallContext *>(cb_args);
  if (!context->ask_q) {
    // Actually an assert as this should never happen
    DEBUG_LOG("Exit AskIngestBulk");
    return;
  }

  NextIngestBulk more = {
      .seqno = context->seqno,
      .from_index = from_index,
      .to_index = to_index,
      .bulk_ctx_id = bulk_ctx_id,
  };

  context->ask_q->push(more);
  char notif = 'm';
  int ret = write(context->fdw, &notif, 1);
  if (ret < 0) {
    DEBUG_LOG("Error number is %d (%s)", errno, strerror(errno));
  }

  DEBUG_LOG("Exit AskIngestBulk");
}

/**
 * Generic callback function that handles completion of a C-SDK operation.
 * The method puts the result data to responses queue, then sends a notification
 * to the Python wrapper.
 */
static void HandleCompletionGeneric(status_code_t status_code, completion_data_t *completion_data,
                                    payload_t response, void *args) {
  auto *context = reinterpret_cast<CallContext *>(args);
  ResponseData data = {
      .seqno = context->seqno,
      .status_code = status_code,
      .response = response,
      .endpoint = {0},
      .protocol_indicator = IsOperationProto(context->op_id) ? PROTOCOL_PROTOBUF : PROTOCOL_JSON,
      .from_index = context->from_index,
      .start_time = context->start_time,
      .elapsed_time = CurrentTimeMicros() - context->start_time,
      .num_reads = context->num_reads,
      .num_writes = context->num_writes,
      .num_qos_retry_considered = 0,
      .num_qos_retry_sent = 0,
      .num_qos_retry_response_used = 0,
  };
  DEBUG_LOG("Operation: %d, PROTO: %d", context->op_id, PROTOCOL_PROTOBUF);

  // Duplicate the endpoint since we must assume the memory will go after this
  // method.
  if (completion_data->endpoint.length > 0) {
    data.endpoint.text = new char[completion_data->endpoint.length + 1];
    memcpy(const_cast<char *>(data.endpoint.text), completion_data->endpoint.text,
        completion_data->endpoint.length + 1);
    data.endpoint.length = completion_data->endpoint.length;
  }
  if (completion_data->qos_retry_considered) {
    data.num_qos_retry_considered = 1;
  }
  if (completion_data->qos_retry_sent) {
    data.num_qos_retry_sent = 1;
  }
  if (completion_data->qos_retry_response_used) {
    data.num_qos_retry_response_used = 1;
  }
  context->responses->push(data);
  char notif = 'x';
  int ret = write(context->fdw, &notif, 1);
  if (ret < 0) {
    // This happens when the session is closed before completing an operation.
    // It happens typically when:
    //   1. Operation is interrupted
    //   2. Then the application/Python closes the session forcefully
    //   3. The unit manager waits for pending operations, but gives up due to
    //   timeout
    //   4. Then the FD is closed already.
    // See also CSdkUnitManager::RemoveSessionProps()
    DEBUG_LOG("Error number is %d (%s)", errno, strerror(errno));
  }
  delete context;
}

/**
 * Method to create a session. New session must call this method first.
 */
static PyObject *CreateSession(PyObject *self, PyObject *args) {
  int session_handle;
  PyObject *fdobj;
  if (!PyArg_ParseTuple(args, "iO:create_session", &session_handle, &fdobj)) {
    return nullptr;
  }
  int fdw = PyObject_AsFileDescriptor(fdobj);
  if (fdw < 0) {
    PyErr_SetString(PyExc_TypeError, "invalid pipe fdw");
    return nullptr;
  }

  DEBUG_LOG("Creating session handle=%d fdw=%d", session_handle, fdw);

  if (!unit_manager->IsValid()) {
    DEBUG_LOG("C-SDK Unit %d is obsolete, old_pid=%d, current_pid=%d", unit_manager->capi(),
              unit_manager->pid(), getpid());
    // recover
    capi_t capi = DaInitialize("");
    auto old_manager = unit_manager->GetNumRegisteredSessions() == 0 ? unit_manager : nullptr;
    unit_manager = new CSdkUnitManager(capi, old_manager);
    if (old_manager != nullptr) {
      DEBUG_LOG("Terminating C-SDK unit %d", old_manager->capi());
      DaTerminate(old_manager->capi());
      delete old_manager;
    }
  }

  // register the session handle
  SessionProperties *props = unit_manager->RegisterSessionProps(session_handle, fdw);
  if (props == nullptr) {
    return nullptr;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject *CloseSession(PyObject *self, PyObject *args) {
  int session_handle;
  int32_t session_id;
  if (!PyArg_ParseTuple(args, "ii:close_session", &session_handle, &session_id)) {
    return nullptr;
  }
  DEBUG_LOG("Closing session handle=%d, session_id=%d", session_handle, session_id);
  SessionProperties *props = unit_manager->GetSessionProps(session_handle, true);
  if (props == nullptr) {
    return nullptr;
  }
  // set closed flag as soon as possible
  props->closed = true;

  if (session_id >= 0) {
    DaCloseSession(props->capi, session_id);
  }

  auto current_unit_manager = props->unit_manager;
  current_unit_manager->RemoveSessionProps(session_handle);
  DEBUG_LOG("Done closing session, manager %p has %ld sessions", current_unit_manager,
            current_unit_manager->GetNumRegisteredSessions());

  DEBUG_LOG("Unit manager pid: %d", current_unit_manager->pid());
  DEBUG_LOG("Registered sessions: %ld", current_unit_manager->GetNumRegisteredSessions());
  if (!current_unit_manager->IsValid() && current_unit_manager->GetNumRegisteredSessions() == 0) {
    DEBUG_LOG("Terminating C-SDK unit %d", current_unit_manager->capi());
    bool is_primary = unit_manager == current_unit_manager;
    if (is_primary) {
      capi_t capi = DaInitialize("");
      unit_manager = new CSdkUnitManager(capi, current_unit_manager->old());
    }
    DEBUG_LOG("Calling DaTerminate");
    DaTerminate(current_unit_manager->capi());
    if (is_primary) {  // TODO(Naoki): Messy. can we do this better?
      delete current_unit_manager;
    } else {
      current_unit_manager->Terminate();
    }
  }

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Method to start the session. New session must call this method first.
 */
static PyObject *StartSession(PyObject *self, PyObject *args) {
  // TODO(TFOS-2139): The first parameter of this method isn't parsed properly
  // for unknown reason. I put a dummy param to workaround the problem for a
  // moment, but we need to know the reason.
  int dummy;
  int session_handle;
  int seqno;
  string_t host;
  int32_t port;
  bool ssl_enabled;
  string_t journal_dir;
  string_t cert_file;
  int64_t operation_timeout_millis;
  if (!PyArg_ParseTuple(args, "iiis#ips#z#l:start_session", &dummy, &session_handle, &seqno,
                        &host.text, &host.length, &port, &ssl_enabled, &journal_dir.text,
                        &journal_dir.length, &cert_file.text, &cert_file.length,
                        &operation_timeout_millis)) {
    return nullptr;
  }

  DEBUG_LOG("Resolving session props for StartSession; handle=%d", session_handle);

  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  if (cert_file.text == nullptr) {
    cert_file.length = 0;
  }

  DEBUG_LOG(
      "Calling StartSession; handle=%d props=%p"
      " seqno=%d fdw=%d host=%s(%d) port=%d ssl_enabled=%d journal=%s(%d) cert=%s(%d)",
      session_handle, props, seqno, props->fdw, host.text, host.length, port, ssl_enabled,
      journal_dir.text, journal_dir.length, cert_file.text, cert_file.length);

  CallContext *context = new CallContext(seqno, props->fdw, props->responses, &props->refcount);

  DaStartSession(props->capi, host, port, ssl_enabled, journal_dir, cert_file,
                 operation_timeout_millis, HandleCompletionGeneric, context);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Login method call.
 */
static PyObject *LoginBIOS(PyObject *self, PyObject *args) {
  int session_handle;
  int seqno;
  int32_t session_id;
  string_t payload_s;
  int32_t disable_routing;
  if (!PyArg_ParseTuple(args, "iiis#p:login_bios", &session_handle, &seqno, &session_id,
                        &payload_s.text, &payload_s.length, &disable_routing)) {
    return nullptr;
  }
  DEBUG_LOG("handle=%d session=%d payload=%s(%d)", session_handle, session_id, payload_s.text,
            payload_s.length);
  payload_t payload = {
      .data = (uint8_t *)payload_s.text,
      .length = payload_s.length,
  };
  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  CallContext *context = new CallContext(seqno, props->fdw, props->responses, &props->refcount);

  DaLoginBIOS(props->capi, session_id, payload, disable_routing, HandleCompletionGeneric, context);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Simple method call.
 */
static PyObject *SimpleMethod(PyObject *self, PyObject *args) {
  int session_handle;
  int seqno;
  int32_t session_id;
  int32_t op_id;
  string_t tenant_s;
  string_t stream_s;
  string_t payload_s;
  string_t endpoint_s;
  if (!PyArg_ParseTuple(args, "iiiiz#z#z#z#:simple_method", &session_handle, &seqno, &session_id,
                        &op_id, &tenant_s.text, &tenant_s.length, &stream_s.text, &stream_s.length,
                        &payload_s.text, &payload_s.length, &endpoint_s.text, &endpoint_s.length)) {
    return nullptr;
  }
  DEBUG_LOG("handle=%d session=%d op_id=%d", session_handle, session_id, op_id);

  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  string_t *tenant = tenant_s.length > 0 ? &tenant_s : nullptr;
  string_t *stream = stream_s.length > 0 ? &stream_s : nullptr;
  payload_t payload = {
      .data = payload_s.length > 0 ? (uint8_t *)payload_s.text : nullptr,
      .length = payload_s.length,
  };
  string_t *endpoint = endpoint_s.length > 0 ? &endpoint_s : nullptr;
  DEBUG_LOG("SimpleMethod tenant=%s stream=%s", tenant_s.text, stream_s.text);
  CallContext *context = new CallContext(seqno, props->fdw, props->responses,
                                         static_cast<CSdkOperationId>(op_id), &props->refcount);

  payload_t *metrics_timestamps = nullptr;
  DaSimpleMethod(props->capi, session_id, static_cast<CSdkOperationId>(op_id), tenant, stream,
                 payload, metrics_timestamps, endpoint, HandleCompletionGeneric, context);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Ingest bulk Start call.
 */
static PyObject *IngestBulkStart(PyObject *self, PyObject *args) {
  int session_handle;
  int seqno;
  int32_t session_id;
  int32_t op_id;
  int num_events;

  if (!PyArg_ParseTuple(args, "iiiii:ingest_bulk_start", &session_handle, &seqno, &session_id,
                        &op_id, &num_events)) {
    return nullptr;
  }
  DEBUG_LOG("Ingest bulk start handle=%d session=%d op_id=%d", session_handle, session_id, op_id);

  // context is needed across multiple calls. So it is registered in a local map
  // Context is erased and deleted when Ingest Bulk ends.
  CallContext *context = unit_manager->RegisterIngestBulkContext(
      seqno, session_handle, static_cast<CSdkOperationId>(op_id));
  if (context == nullptr) {
    return nullptr;
  }

  DaIngestBulkStart(unit_manager->capi(), session_id, static_cast<CSdkOperationId>(op_id),
                    num_events, HandleCompletionGeneric, AskIngestBulk, context);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Ingest bulk call.
 */
static PyObject *IngestBulk(PyObject *self, PyObject *args) {
  int session_handle;
  int seqno;
  int32_t session_id;
  int64_t bulk_ctx_id;
  int from_index;
  int to_index;
  PyObject *resources_obj;
  PyObject *options_obj;
  string_t payload_s;

  if (!PyArg_ParseTuple(args, "iiiliiO!O!z#:ingest_bulk", &session_handle, &seqno, &session_id,
                        &bulk_ctx_id, &from_index, &to_index, &PyList_Type, &resources_obj,
                        &PyList_Type, &options_obj, &payload_s.text, &payload_s.length)) {
    return nullptr;
  }
  DEBUG_LOG("Ingest bulk handle=%d session=%d from=%d to=%d bulk_id=%ld", session_handle,
            session_id, from_index, to_index, bulk_ctx_id);

  CallContext *master_context = unit_manager->GetIngestBulkContext(seqno, session_handle);
  if (master_context == nullptr) {
    return nullptr;
  }

  CallContext *context = new CallContext(*master_context);
  context->from_index = from_index;

  int num_resources = PyList_Size(resources_obj);
  string_t resources[num_resources + 1];
  for (int i = 0; i < num_resources; ++i) {
    PyObject *str = PyList_GetItem(resources_obj, i);
    const char *item = PyUnicode_AsUTF8(str);
    if (item == nullptr) {
      char buf[1024];
      snprintf(buf, sizeof(buf), "resources[%d] is not str type", i);
      PyErr_SetString(PyExc_TypeError, buf);
      return nullptr;
    }
    resources[i].text = item;
    resources[i].length = strlen(item);
    DEBUG_LOG("resources[%d]=%s", i, resources[i].text);
  }
  resources[num_resources] = {0};

  // TODO(Naoki): Make this a subroutine
  int num_options = PyList_Size(options_obj);
  string_t options[num_options + 1];
  DEBUG_LOG("options.size = %d", num_options);
  for (int i = 0; i < num_options; ++i) {
    PyObject *str = PyList_GetItem(options_obj, i);
    const char *item = PyUnicode_AsUTF8(str);
    if (item == nullptr) {
      char buf[1024];
      snprintf(buf, sizeof(buf), "options[%d] is not str type", i);
      PyErr_SetString(PyExc_TypeError, buf);
      return nullptr;
    }
    options[i].text = item;
    options[i].length = strlen(item);
    DEBUG_LOG("options[%d]=%s", i, options[i].text);
  }
  options[num_options] = {0};

  payload_t payload = {
      .data = payload_s.length > 0 ? (uint8_t *)payload_s.text : nullptr,
      .length = payload_s.length,
  };

  payload_t *metrics_timestamps = nullptr;
  DaIngestBulk(unit_manager->capi(), session_id, bulk_ctx_id, from_index, to_index, resources,
               options, payload, metrics_timestamps, HandleCompletionGeneric, context);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Ingest bulk end call.
 */
static PyObject *IngestBulkEnd(PyObject *self, PyObject *args) {
  int session_handle;
  int seqno;
  int32_t session_id;
  int64_t bulk_ctx_id;

  if (!PyArg_ParseTuple(args, "iiil:ingest_bulk_end", &session_handle, &seqno, &session_id,
                        &bulk_ctx_id)) {
    return nullptr;
  }
  DEBUG_LOG("Ingest bulk End handle=%d session=%d id=%ld", session_handle, session_id, bulk_ctx_id);

  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  DaIngestBulkEnd(props->capi, session_id, bulk_ctx_id);

  unit_manager->RemoveIngestBulkContext(seqno);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Generic method call.
 */
static PyObject *GenericMethod(PyObject *self, PyObject *args) {
  int session_handle;
  int seqno;
  int32_t session_id;
  int32_t op_id;
  PyObject *resources_obj;
  PyObject *options_obj;
  string_t payload_s;
  int64_t operation_timeout_millis;
  if (!PyArg_ParseTuple(args, "iiiiO!O!z#l:generic_method", &session_handle, &seqno, &session_id,
                        &op_id, &PyList_Type, &resources_obj, &PyList_Type, &options_obj,
                        &payload_s.text, &payload_s.length, &operation_timeout_millis)) {
    return nullptr;
  }
  DEBUG_LOG("GenericMethod handle=%d session=%d op_id=%d", session_handle, session_id, op_id);
  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  int num_resources = PyList_Size(resources_obj);
  string_t resources[num_resources + 1];
  for (int i = 0; i < num_resources; ++i) {
    PyObject *str = PyList_GetItem(resources_obj, i);
    const char *item = PyUnicode_AsUTF8(str);
    if (item == nullptr) {
      char buf[1024];
      snprintf(buf, sizeof(buf), "resources[%d] is not str type", i);
      PyErr_SetString(PyExc_TypeError, buf);
      return nullptr;
    }
    resources[i].text = item;
    resources[i].length = strlen(item);
    DEBUG_LOG("resources[%d]=%s", i, resources[i].text);
  }
  resources[num_resources] = {0};

  // TODO(Naoki): Make this a subroutine
  int num_options = PyList_Size(options_obj);
  string_t options[num_options + 1];
  DEBUG_LOG("options.size = %d", num_options);
  for (int i = 0; i < num_options; ++i) {
    PyObject *str = PyList_GetItem(options_obj, i);
    const char *item = PyUnicode_AsUTF8(str);
    if (item == nullptr) {
      char buf[1024];
      snprintf(buf, sizeof(buf), "options[%d] is not str type", i);
      PyErr_SetString(PyExc_TypeError, buf);
      return nullptr;
    }
    options[i].text = item;
    options[i].length = strlen(item);
    DEBUG_LOG("options[%d]=%s", i, options[i].text);
  }
  options[num_options] = {0};

  payload_t payload = {
      .data = payload_s.length > 0 ? (uint8_t *)payload_s.text : nullptr,
      .length = payload_s.length,
  };
  CallContext *context = new CallContext(seqno, props->fdw, props->responses,
                                         static_cast<CSdkOperationId>(op_id), &props->refcount);

  DaGenericMethod(props->capi, session_id, static_cast<CSdkOperationId>(op_id),
                  num_resources > 0 ? resources : nullptr, num_options > 0 ? options : nullptr,
                  payload, operation_timeout_millis, nullptr, HandleCompletionGeneric, context);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Calls C-SDK method to add an endpoint.
 */
static PyObject *AddEndpoint(PyObject *self, PyObject *args) {
  int session_handle;
  int seqno;
  int32_t session_id;
  string_t endpoint;
  int node_type;
  if (!PyArg_ParseTuple(args, "iiiz#i:add_endpoint", &session_handle, &seqno, &session_id,
                        &endpoint.text, &endpoint.length, &node_type)) {
    return nullptr;
  }
  DEBUG_LOG("handle=%d session=%d endpoint=%s node_type=%d", session_handle, session_id,
            endpoint.text, node_type);

  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  CallContext *context = new CallContext(seqno, props->fdw, props->responses, &props->refcount);

  DaAddEndpoint(props->capi, session_id, endpoint, static_cast<CSdkNodeType>(node_type),
                HandleCompletionGeneric, context);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Method that returns next set of response data.
 * Return value is a large tuple (see code below for tuple elements)).
 * If there is no response in the queue, the method returns None.
 */
static PyObject *FetchNext(PyObject *self, PyObject *args) {
  int session_handle;
  if (!PyArg_ParseTuple(args, "i:fetch_next", &session_handle)) {
    return nullptr;
  }
  DEBUG_LOG("session=%d", session_handle);
  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  ResponseData data;
  if (!props->responses->pop(data)) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  DEBUG_LOG("endpoint=%s (%d)", data.endpoint.text, data.endpoint.length);

  const char *format = (data.status_code == 0 && data.protocol_indicator == PROTOCOL_PROTOBUF)
                           ? "(iiy#z#ikkkkkkk)"
                           : "(iis#z#ikkkkkkk)";
  PyObject *response =
      Py_BuildValue(format, data.seqno, data.status_code, data.response.data, data.response.length,
                    data.endpoint.text, data.endpoint.length, data.from_index, data.start_time,
                    data.elapsed_time, data.num_reads, data.num_writes,
                    data.num_qos_retry_considered, data.num_qos_retry_sent,
                    data.num_qos_retry_response_used);
  CsdkReleasePayload(&data.response);
  delete[] data.endpoint.text;
  return response;
}

/**
 * Method that returns information about the sizing for the next bulk batch
 */
static PyObject *FetchNextBulk(PyObject *self, PyObject *args) {
  int session_handle;
  if (!PyArg_ParseTuple(args, "i:fetch_next_bulk", &session_handle)) {
    return nullptr;
  }
  DEBUG_LOG("Fetching Next bulk -- session=%d", session_handle);
  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  NextIngestBulk more;
  if (!props->ask_q->pop(more)) {
    Py_INCREF(Py_None);
    return Py_None;
  }
  DEBUG_LOG("Fetching Next bulk -- seqno=%d, bulk_ctx=%ld, from=%d, to=%d", more.seqno,
            more.bulk_ctx_id, more.from_index, more.to_index);

  PyObject *response =
      Py_BuildValue("(ilii)", more.seqno, more.bulk_ctx_id, more.from_index, more.to_index);
  return response;
}

#ifdef DEBUG_LOG_ENABLED
static int Clear(PyObject *) {
  DEBUG_LOG("Clear called");
  return 0;
}
#endif

static void Terminate(void *) {
  DEBUG_LOG("Terminate called");
  DaTerminate(unit_manager->capi());
  delete unit_manager;
}

/**
 * Test method that returns a "hello world" message.
 */
static PyObject *HelloWorld(PyObject *self, PyObject *args) {
  payload_t payload = CsdkAllocatePayload(128);
  CsdkWriteHello(&payload);
  PyObject *result = Py_BuildValue("s", reinterpret_cast<char *>(payload.data));
  CsdkReleasePayload(&payload);
  return result;
}

/**
 * Method that returns the name of op_id.
 * This method is useful for testing wrapper/C-SDK operation ID consistency.
 */
static PyObject *GetOperationName(PyObject *self, PyObject *args) {
  int32_t id;
  PyArg_ParseTuple(args, "i", &id);
  string_t result = CsdkGetOperationName(static_cast<CSdkOperationId>(id));
  return Py_BuildValue("s#", result.text, result.length);
}

static PyObject *ListStatusCodes(PyObject *self, PyObject *args) {
  payload_t data = CsdkListStatusCodes();
  int num_elements = data.length / sizeof(status_code_t);
  PyObject *result = PyList_New(num_elements);
  if (result == nullptr) {
    return nullptr;
  }
  for (int i = 0; i < num_elements; ++i) {
    status_code_t status_code;
    status_code = *reinterpret_cast<status_code_t *>(&data.data[i * sizeof(status_code_t)]);
    PyObject *python_int = Py_BuildValue("i", status_code);
    PyList_SetItem(result, i, python_int);
  }
  CsdkReleasePayload(&data);
  return result;
}

/**
 * Method that returns the name of a status number.
 * This method is useful for testing wrapper/C-SDK status code consistency.
 */
static PyObject *GetStatusName(PyObject *self, PyObject *args) {
  status_code_t status_code;
  PyArg_ParseTuple(args, "i", &status_code);
  string_t result = CsdkGetStatusName(status_code);
  DEBUG_LOG("name=%s (%d)", result.text, result.length);
  return Py_BuildValue("s#", result.text, result.length);
}

struct EchoContext {
  int seqno;
  SessionProperties *props;
  payload_t message;
  int delay_ms;
  bool binary_data_;
  int from_index;
  int to_index;

  EchoContext(int seqno, SessionProperties *props, const payload_t &message, int delay_ms,
              bool binary)
      : seqno(seqno),
        props(props),
        message(message),
        delay_ms(delay_ms),
        binary_data_(binary > 0),
        from_index(0),
        to_index(0) {}

  EchoContext(int seqno, SessionProperties *props, int from_index, int to_index)
      : seqno(seqno),
        props(props),
        message({0}),
        delay_ms(0),
        binary_data_(false),
        from_index(from_index),
        to_index(to_index) {}
};

void *RunEcho(void *arg) {
  auto *context = reinterpret_cast<EchoContext *>(arg);
  usleep(context->delay_ms * 1000);
  status_code_t status_code = 0;
  ResponseData data = {
      .seqno = context->seqno,
      .status_code = status_code,
      .response = context->message,
      .endpoint = {0},
      .protocol_indicator = (context->binary_data_) ? PROTOCOL_PROTOBUF : PROTOCOL_JSON,
      .from_index = 0,
  };
  context->props->responses->push(data);
  char notif = 'x';
  int ret = write(context->props->fdw, &notif, 1);
  if (ret < 0) {
    // TODO(Naoki): This is unlikely to happen, but how to handle this properly?
    DEBUG_LOG("Error number is %d (%s)", errno, strerror(errno));
  }
  delete context;
  return nullptr;
}

void *RunAsk(void *arg) {
  auto *context = reinterpret_cast<EchoContext *>(arg);

  NextIngestBulk more = {
      .seqno = context->seqno,
      .from_index = context->from_index,
      .to_index = context->to_index,
      .bulk_ctx_id = 1,
  };

  context->props->ask_q->push(more);
  char notif = 'm';
  int ret = write(context->props->fdw, &notif, 1);
  if (ret < 0) {
    DEBUG_LOG("Error number is %d (%s)", errno, strerror(errno));
  }
  delete context;
  return nullptr;
}

/**
 * Echo operation for testing.
 */
static PyObject *Echo(PyObject *self, PyObject *args) {
  int session_handle;
  int seqno;
  string_t message;
  int delay_ms;
  uint8_t protocol_indicator = 0;
  if (!PyArg_ParseTuple(args, "iis#i|b:echo", &session_handle, &seqno, &message.text,
                        &message.length, &delay_ms, &protocol_indicator)) {
    return nullptr;
  }
  DEBUG_LOG("binary=%d, session_handle=%d", protocol_indicator, session_handle);
  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  payload_t message_payload = {0};
  if (message.length > 0) {
    message_payload.data = new uint8_t[message.length + 1];
    memcpy(message_payload.data, message.text, message.length);
    message_payload.data[message.length] = 0;
    message_payload.length = message.length;
  }
  auto *context = new EchoContext(seqno, props, message_payload, delay_ms, protocol_indicator);

  pthread_t pid;
  pthread_create(&pid, nullptr, RunEcho, context);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Ask operation for testing.
 */
static PyObject *Ask(PyObject *self, PyObject *args) {
  int session_handle;
  int seqno;
  int from_index;
  int to_index;

  if (!PyArg_ParseTuple(args, "iiii:echo", &session_handle, &seqno, &from_index, &to_index)) {
    return nullptr;
  }
  DEBUG_LOG("session_handle=%d, from=%d, to=%d", session_handle, from_index, to_index);
  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  auto *context = new EchoContext(seqno, props, from_index, to_index);

  pthread_t pid;
  pthread_create(&pid, nullptr, RunAsk, context);

  Py_INCREF(Py_None);
  return Py_None;
}

/**
 * Log an event
 */
static PyObject *Log(PyObject *self, PyObject *args) {
  int session_handle;
  int32_t session_id;
  uint64_t event_time;
  uint64_t latency_internal_us;
  uint64_t latency_us;
  uint64_t num_reads;
  uint64_t num_writes;
  const char *request;
  const char *status;
  const char *message;
  const char *server_endpoint;
  const char *stream;
  uint64_t num_qos_retry_considered;
  uint64_t num_qos_retry_sent;
  uint64_t num_qos_retry_response_used;

  if (!PyArg_ParseTuple(args, "iikkkkkssssskkk:log", &session_handle, &session_id, &event_time,
                        &latency_internal_us, &latency_us, &num_reads, &num_writes, &request,
                        &status, &message, &server_endpoint, &stream, &num_qos_retry_considered,
                        &num_qos_retry_sent, &num_qos_retry_response_used)) {
    return nullptr;
  }
  DEBUG_LOG("session_handle=%d", session_handle);
  SessionProperties *props = unit_manager->GetSessionProps(session_handle);
  if (props == nullptr) {
    return nullptr;
  }

  DaLog(props->capi, session_id, stream, request, status, server_endpoint, latency_us,
        latency_internal_us, num_reads, num_writes, num_qos_retry_considered, num_qos_retry_sent,
        num_qos_retry_response_used);

  Py_INCREF(Py_None);
  return Py_None;
}

// Our Module's Function Definition struct
// We require this `NULL` to signal the end of our method
// definition
static PyMethodDef myMethods[] = {
    {"create_session", CreateSession, METH_VARARGS, "Create a new session"},
    {"close_session", CloseSession, METH_VARARGS, "Close a session"},
    {"start_session", StartSession, METH_VARARGS, "Start session"},
    {"login_bios", LoginBIOS, METH_VARARGS, "Bios Login operation"},
    {"simple_method", SimpleMethod, METH_VARARGS, "Execute a simple operation"},
    {"generic_method", GenericMethod, METH_VARARGS, "Execute a generic operation"},
    {"add_endpoint", AddEndpoint, METH_VARARGS, "Add endpoint"},
    {"fetch_next", FetchNext, METH_VARARGS, "Fetch next available response data"},
    {"hello_world", HelloWorld, METH_NOARGS, "Calls C-SDK hello world method"},
    {"get_operation_name", GetOperationName, METH_VARARGS,
     "Returns method name for the specified ID"},
    {"list_status_codes", ListStatusCodes, METH_NOARGS, "List all status codes"},
    {"get_status_name", GetStatusName, METH_VARARGS, "Returns status name for the specified ID"},
    {"echo", Echo, METH_VARARGS, "Echo method for testing, takes message and delay ms"},
    {"ingest_bulk_start", IngestBulkStart, METH_VARARGS, "Start an Ingest Bulk Operation"},
    {"ingest_bulk", IngestBulk, METH_VARARGS, "Do Part of the Actual Ingest Bulk"},
    {"ingest_bulk_end", IngestBulkEnd, METH_VARARGS, "End an Ingest Bulk Operation"},
    {"fetch_next_bulk", FetchNextBulk, METH_VARARGS, "Fetch the next bulk"},
    {"ask", Ask, METH_VARARGS, "Interface for testing bulk interactions"},
    {"log", Log, METH_VARARGS, "Log an event"},
    {NULL, NULL, 0, NULL}};

// Our Module Definition struct
static struct PyModuleDef csdk_module = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "_csdk",
    .m_doc = "TFOS C-SDK library v" BIOS_VERSION,
    .m_size = -1,
    .m_methods = myMethods,
    .m_slots = nullptr,
    .m_traverse = nullptr,
#ifdef DEBUG_LOG_ENABLED
    .m_clear = Clear,
#else
    .m_clear = nullptr,
#endif
    .m_free = Terminate,
};

// Initializes our module using our above struct
PyMODINIT_FUNC PyInit__bios_csdk_native(void) {
  Py_Initialize();
  DEBUG_LOG("Initializing bios module");
  capi_t capi = DaInitialize("");
  unit_manager = new CSdkUnitManager(capi);
  return PyModule_Create(&csdk_module);
}
