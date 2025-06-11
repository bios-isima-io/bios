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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a parquet writer that opens a unique pipe to write it to a memory stream through jni
 * (using arrow to parquet native library).
 */
public class ParquetWriter implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ParquetWriter.class);
  private static final long INVALID_HANDLE = -1L;

  private final ParquetJniWrapper wrapper;
  private final long writeHandle;

  public ParquetWriter(Schema schema) {
    this.wrapper = ParquetJniWrapper.getInstance();
    long tmpHandle;
    try {
      tmpHandle = this.wrapper.openWriter(schema);
    } catch (IOException e) {
      // defer failure to the write call
      logger.error("Unable to load parquet jni library: {}", e.getMessage());
      tmpHandle = INVALID_HANDLE;
    }
    writeHandle = tmpHandle;
  }

  @Override
  public void close() {
    if (writeHandle != INVALID_HANDLE) {
      wrapper.destroyWriter(writeHandle);
    }
  }

  public void writeNext(ArrowRecordBatch recordBatch) throws IOException {
    if (writeHandle == INVALID_HANDLE) {
      logger.error("Parquet writer is either not initialized or is not writable anymore");
      throw new IOException("Parquet Writer is not writable anymore");
    }
    wrapper.writeNext(writeHandle, recordBatch);
  }

  public ByteBuffer closeAndGetBuffer() {
    return wrapper.closeWriter(writeHandle);
  }
}
