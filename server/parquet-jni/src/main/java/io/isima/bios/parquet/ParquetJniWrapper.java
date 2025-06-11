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
package io.isima.bios.parquet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetJniWrapper {
  private static final Logger logger = LoggerFactory.getLogger(ParquetJniWrapper.class);
  private static ParquetJniWrapper wrapper;

  static {
    ParquetJniLoader.getInstance();
  }

  static ParquetJniWrapper getInstance() {
    if (wrapper == null) {
      wrapper = new ParquetJniWrapper();
    }
    return wrapper;
  }

  private ParquetJniWrapper() {}

  public long openWriter(Schema arrowSchema) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final var writableChannel = Channels.newChannel(out);
    final var writeChannel = new WriteChannel(writableChannel);
    MessageSerializer.serialize(writeChannel, arrowSchema);
    byte[] arrowSchemaBytes = out.toByteArray();
    return openParquetWriter(arrowSchemaBytes);
  }

  void destroyWriter(long writeHandle) {
    destroyParquetWriter(writeHandle);
  }

  void writeNext(long writeHandle, ArrowRecordBatch recordBatch) {
    logger.debug("Writing next batch; hdl = {} : rows = {}", writeHandle, recordBatch.getLength());
    final int numRows = recordBatch.getLength();
    final List<ArrowBuf> buffers = recordBatch.getBuffers();
    final List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
    final int bufSize = buffers.size();
    if (bufSize == 0 || bufSize != buffersLayout.size()) {
      // this is unexpected
      throw new RuntimeException(
          "Unexpected deviation in sizes " + bufSize + ":" + buffersLayout.size());
    }

    // layout and buffers is of the same size
    long[] bufAddrs = new long[bufSize];
    long[] bufSizes = new long[bufSize];

    for (int i = 0; i < bufSize; i++) {
      final var buf = buffers.get(i);
      final var bufLayout = buffersLayout.get(i);
      bufAddrs[i] = buf.memoryAddress();
      bufSizes[i] = bufLayout.getSize();
    }
    writeNext(writeHandle, numRows, bufAddrs, bufSizes);
  }

  public ByteBuffer closeWriter(long writeHandle) {
    return closeParquetWriter(writeHandle);
  }

  private native long openParquetWriter(byte[] schemaBytes);

  private native void destroyParquetWriter(long id);

  private native void writeNext(long id, int numRows, long[] bufAddrs, long[] bufSizes);

  private native ByteBuffer closeParquetWriter(long id);
}
