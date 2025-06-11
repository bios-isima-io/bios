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
package io.isima.bios.sdk.csdk;

import io.isima.bios.models.SessionInfo;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.impl.BiosClientUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CSdkDirect {
  private static final int capi;

  static {
    final String libname = "csdkdirect";
    if (!EmbeddedLibLoader.load("lib" + libname)) {
      String libpath = System.getProperty("java.library.path");
      System.out.println("java.library.path=" + libpath);
      System.loadLibrary(libname);
    }
    CSdkDirect init = new CSdkDirect();
    capi =
        init.initialize(
            CallbackCenter.class,
            System.getProperty("com.tieredfractals.tfos.csdk.direct.modules-profile", ""));
    if (capi < 0) {
      throw new RuntimeException("Initializing " + libname + " failed");
    }
  }

  public static int getCapi() {
    return capi;
  }

  /**
   * The method to check whether the session has permission to run the specified operation.
   *
   * @param sessionId Session ID to check permission
   * @param opId The target operation
   * @throws BiosClientException Thrown when the operation is not allowed to be executed.
   */
  public void checkOperationPermission(int sessionId, CSdkOperationId opId)
      throws BiosClientException {
    try {
      checkOperationPermissionNative(sessionId, opId.value());
    } catch (CSdkException e) {
      throw new BiosClientException(
          BiosClientUtils.statusToError(e.getStatusCode()), e.getMessage());
    }
  }

  // Native methods ///////////////////////////////

  /**
   * Initializes the native client.
   *
   * @param callbackCenterClass CallbackCenter class object. This is used by the JNI layer to invoke
   *     callbacks from C-SDK.
   * @param modulesProfile "" for default, "mock" for mock testing without going to server.
   * @return 0 on successful execution, non-zero otherwise.
   */
  private native int initialize(Class<?> callbackCenterClass, String modulesProfile);

  public native boolean isInitialized();

  public native void startSession(
      String host,
      int port,
      boolean sslEnabled,
      String journalDir,
      String cafile,
      long operationTimeoutMillis,
      CSdkApiCaller<StartSessionResponse> callbacks);

  /**
   * Method to request C-SDK to login to the specified scope and start a session.
   *
   * @param payload Body of serialized credentials.
   * @param length Length of serialized credentials.
   * @param call AsyncNativeCall object to receive the completion callback. The callback receives
   *     session ID if the login was successful.
   */
  public native void loginBios(
      int sessionId, ByteBuffer payload, long length, CSdkApiCaller<SessionInfo> call);

  public native void simpleMethod(
      int sessionId,
      int operationId,
      String tenantName,
      String streamName,
      ByteBuffer payload,
      long length,
      CSdkApiCaller<?> call,
      ByteBuffer metricTimestamps);

  public native void genericMethod(
      int sessionId,
      int operationId,
      String[] resources,
      String[] options,
      ByteBuffer payload,
      long length,
      CSdkApiCaller<?> call,
      ByteBuffer metricTimestamps);

  @SuppressWarnings("rawtypes")
  public native long ingestBulkStart(
      int sessionId, int operationId, int size, InsertBulkExecutor pagingCallback);

  public native void ingestBulk(
      int sessionId,
      long contextId,
      int fromIndex,
      int toIndex,
      String[] options,
      ByteBuffer payload,
      long length,
      CSdkApiCaller<?> call,
      ByteBuffer metricTimestamps);

  public native void ingestBulkEnd(long operationCtx, int sessionId, long contextId);

  private native void checkOperationPermissionNative(int sessionId, int operationId)
      throws CSdkException;

  public native void endSession(int sessionId);

  /**
   * Requests the native space to allocate memory for a byte buffer.
   *
   * @param capacity Required capacityy
   * @return Direct byte buffer with allocated memory
   */
  public native ByteBuffer allocateBuffer(int capacity);

  /**
   * Releases memory allocated for the specified payload by C-SDK.
   *
   * <p>Never use this method with a ByteBuffer allocated on Java side. It would cause process
   * crash.
   *
   * @param payload Payload to release.
   */
  public native void releasePayload(ByteBuffer payload);

  /**
   * Adds record to _clientMetrics signal.
   *
   * @param sessionId session ID
   * @param stream stream
   * @param request request
   * @param status status
   * @param serverEndpoint server endpoint
   * @param latencyUs wrapper latency in microseconds
   * @param latencyInternalUs csdk latency in microseconds
   * @param numReads number of reads
   * @param numWrites number of writes
   * @param qosRetryConsidered Number of operations that considered QoS retry
   * @param qosRetrySent Number of QoS retries
   * @param qosRetryResponseUsed Number of operations that response for QoS retry was used
   */
  public native void log(
      int sessionId,
      String stream,
      String request,
      String status,
      String serverEndpoint,
      long latencyUs,
      long latencyInternalUs,
      long numReads,
      long numWrites,
      long qosRetryConsidered,
      long qosRetrySent,
      long qosRetryResponseUsed);

  public native long currentTimeMillis();

  // Test utilities /////////////////////////////////

  public native String getOperationName(int id);

  private native int[] listStatusCodesNative();

  public native String getStatusName(int status);

  public void writeHello(ByteBuffer buffer) {
    int limit = writeHelloNative(buffer);
    buffer.clear();
    buffer.limit(limit);
  }

  public List<BiosClientError> listStatusCodes() throws BiosClientException {
    List<BiosClientError> errors = new ArrayList<>();
    for (int statusCode : listStatusCodesNative()) {
      final BiosClientError err = BiosClientUtils.statusToError(statusCode);
      errors.add(err);
    }
    return errors;
  }

  /**
   * Test method to write "hello world" to the specified byte buffer.
   *
   * @param buffer Buffer to be written. Capacity should be equal to or more than 11, otherwise the
   *     string would be truncated.
   */
  private native int writeHelloNative(ByteBuffer buffer);
}
