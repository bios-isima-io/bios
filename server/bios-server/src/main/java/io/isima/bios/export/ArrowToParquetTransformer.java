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
package io.isima.bios.export;

import static io.isima.bios.export.ArrowDataPipe.RecordBatch;

import io.isima.bios.parquet.ParquetWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Arrow to parquet transformer.
 *
 * <p>For a given instance of the transformer, all methods in this object is considered to be called
 * by a single thread. Due to this, this class is not thread safe.
 */
public class ArrowToParquetTransformer implements DataProvider<ByteBuffer> {
  private static final Logger logger = LoggerFactory.getLogger(ArrowToParquetTransformer.class);

  private final ArrowDataPipe dataPipe;
  private final String tenantName;
  private final String signalName;
  private final long signalVersion;

  private final AtomicReference<DataSink<ByteBuffer>> parquetDataSink = new AtomicReference<>();

  // the following variables are assumed to be processed in the context of a single worker thread
  private ParquetWriter currentWriter;
  private ParquetWriter prevWriter;
  private long batchTimeStamp;

  public ArrowToParquetTransformer(
      String tenantName, String signalName, long signalVersion, ArrowDataPipe dataPipe) {
    Objects.requireNonNull(tenantName);
    Objects.requireNonNull(signalName);
    this.tenantName = tenantName;
    this.signalName = signalName;
    this.signalVersion = signalVersion;
    this.dataPipe = dataPipe;
    this.currentWriter = new ParquetWriter(dataPipe.getArrowSchema());
    this.dataPipe.addBatchConsumer(this::transformBatch);
    this.batchTimeStamp = 0L;
    this.prevWriter = null;
  }

  public void transformBatch(RecordBatch recordBatch) {
    try {
      this.currentWriter.writeNext(recordBatch.getArrowRecordBatch());
      if (this.batchTimeStamp == 0L) {
        this.batchTimeStamp = recordBatch.getFirstEventTimeStamp();
      }
    } catch (IOException e) {
      // currently just warn..we may need to do something better in the future
      logger.warn("Unable to transform data to arrow : {}", e.getMessage());
    }
  }

  /**
   * Consumes the piled up parquet data.
   *
   * <p>Called when we are done with one parquet file and wants to flush this to the cloud (or local
   * file system). After this call a new writer is created to consume the next set of batches.
   */
  @Override
  public boolean flushDataToSink() {
    if (parquetDataSink.get() == null) {
      // no one has registered for data
      return false;
    }
    if (this.batchTimeStamp == 0) {
      // no batches yet
      return false;
    }
    if (this.prevWriter != null) {
      // only one flush at a time
      return false;
    }
    // this is asynchronous
    this.parquetDataSink.get().onData(this.currentWriter.closeAndGetBuffer(), this.batchTimeStamp);
    this.prevWriter = currentWriter;
    this.currentWriter = new ParquetWriter(dataPipe.getArrowSchema());
    this.batchTimeStamp = 0;
    return true;
  }

  @Override
  public void sinkFlushed() {
    if (this.prevWriter != null) {
      this.prevWriter.close();
      this.prevWriter = null;
    }
  }

  @Override
  public DataPipe getDataPipe() {
    return dataPipe;
  }

  @Override
  public String getTenantName() {
    return tenantName;
  }

  @Override
  public String getSignalName() {
    return signalName;
  }

  @Override
  public long getSignalVersion() {
    return signalVersion;
  }

  @Override
  public void registerDataSink(DataSink<ByteBuffer> sink) {
    this.parquetDataSink.set(Objects.requireNonNull(sink));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArrowToParquetTransformer that = (ArrowToParquetTransformer) o;
    return tenantName.equals(that.tenantName)
        && signalName.equals(that.signalName)
        && signalVersion == that.signalVersion;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantName, signalName, signalVersion);
  }
}
