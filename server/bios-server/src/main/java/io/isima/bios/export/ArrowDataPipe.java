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

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.MessageType;

/**
 * Pipes arrow data.
 *
 * <p>Externally, a worker thread can consume data from this pipe by registering one or more
 * consumers. It is assumed that a single worker thread drains the pipe and clears the drain.
 */
public class ArrowDataPipe implements DataPipe {
  private final Deque<RecordBatch> dataPipe;
  private final List<ArrowRecordBatch> drainedBatches;
  private final SchemaMapping schemaMapping;
  private final List<Consumer<RecordBatch>> batchConsumers;

  ArrowDataPipe(SchemaMapping schemaMapping) {
    this.schemaMapping = schemaMapping;
    this.dataPipe = new LinkedBlockingDeque<>();
    this.batchConsumers = new CopyOnWriteArrayList<>();
    this.drainedBatches = new ArrayList<>();
  }

  public void addBatchConsumer(Consumer<RecordBatch> batchConsumer) {
    batchConsumers.add(batchConsumer);
  }

  public void addToBatch(VectorSchemaRoot schemaRoot, long firstTimeStamp) {
    final var ret = createBatch(schemaRoot);
    if (ret != null) {
      dataPipe.add(new RecordBatch(ret, firstTimeStamp));
    }
  }

  @Override
  public void drainPipe() {
    while (true) {
      final var next = dataPipe.poll();
      if (next == null) {
        break;
      }
      for (final var consumer : batchConsumers) {
        consumer.accept(next);
      }
      drainedBatches.add(next.getArrowRecordBatch());
    }
  }

  @Override
  public void clearDrain() {
    drainedBatches.forEach(ArrowRecordBatch::close);
    drainedBatches.clear();
  }

  public Schema getArrowSchema() {
    return schemaMapping.getArrowSchema();
  }

  public MessageType getParquetSchema() {
    return schemaMapping.getParquetSchema();
  }

  private ArrowRecordBatch createBatch(VectorSchemaRoot root) {
    if (root.getRowCount() <= 0) {
      return null;
    }
    VectorUnloader unloader = new VectorUnloader(root);
    return unloader.getRecordBatch();
  }

  public static final class RecordBatch {
    private final ArrowRecordBatch arrowRecordBatch;
    private final long firstEventTimeStamp;

    public RecordBatch(ArrowRecordBatch arrowRecordBatch, long firstEventTimeStamp) {
      this.arrowRecordBatch = arrowRecordBatch;
      this.firstEventTimeStamp = firstEventTimeStamp;
    }

    public ArrowRecordBatch getArrowRecordBatch() {
      return arrowRecordBatch;
    }

    public long getFirstEventTimeStamp() {
      return firstEventTimeStamp;
    }
  }
}
