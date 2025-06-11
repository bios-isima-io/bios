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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

public interface Vectorizer<BiosT> {
  void vectorize(int index, BiosT value);

  Class<BiosT> getType();

  void preAllocate(int valueCount);

  final class LongVectorizer implements Vectorizer<Long> {
    private final BigIntVector fv;

    LongVectorizer(BigIntVector biv) {
      this.fv = biv;
    }

    @Override
    public void vectorize(int index, Long value) {
      fv.set(index, value);
    }

    @Override
    public Class<Long> getType() {
      return Long.class;
    }

    @Override
    public void preAllocate(int valueCount) {
      fv.allocateNew(valueCount);
    }
  }

  final class DoubleVectorizer implements Vectorizer<Double> {
    private final Float8Vector fv;

    DoubleVectorizer(Float8Vector f8v) {
      this.fv = f8v;
    }

    @Override
    public void vectorize(int index, Double value) {
      fv.set(index, value);
    }

    @Override
    public Class<Double> getType() {
      return Double.class;
    }

    @Override
    public void preAllocate(int valueCount) {
      fv.allocateNew(valueCount);
    }
  }

  final class BooleanVectorizer implements Vectorizer<Boolean> {
    private final BitVector fv;

    BooleanVectorizer(BitVector bv) {
      this.fv = bv;
    }

    @Override
    public void vectorize(int index, Boolean value) {
      fv.set(index, value ? 1 : 0);
    }

    @Override
    public Class<Boolean> getType() {
      return Boolean.class;
    }

    @Override
    public void preAllocate(int valueCount) {
      fv.allocateNew(valueCount);
    }
  }

  final class StringVectorizer implements Vectorizer<String> {
    private final VarCharVector fv;

    StringVectorizer(VarCharVector vcv) {
      this.fv = vcv;
    }

    @Override
    public void vectorize(int index, String value) {
      fv.setSafe(index, value.getBytes());
    }

    @Override
    public Class<String> getType() {
      return String.class;
    }

    @Override
    public void preAllocate(int valueCount) {
      fv.allocateNew(valueCount);
    }
  }

  final class BlobVectorizer implements Vectorizer<ByteBuffer> {
    private final VarBinaryVector fv;

    BlobVectorizer(VarBinaryVector vbv) {
      this.fv = vbv;
    }

    @Override
    public void vectorize(int index, ByteBuffer value) {
      fv.setSafe(index, value.array());
    }

    @Override
    public Class<ByteBuffer> getType() {
      return ByteBuffer.class;
    }

    @Override
    public void preAllocate(int valueCount) {
      fv.allocateNew(valueCount);
    }
  }

  final class TimeStampVectorizer implements Vectorizer<Long> {
    private final TimeStampMilliVector fv;

    TimeStampVectorizer(TimeStampMilliVector tsmv) {
      this.fv = tsmv;
    }

    @Override
    public void vectorize(int index, Long value) {
      fv.setSafe(index, value);
    }

    @Override
    public Class<Long> getType() {
      return Long.class;
    }

    @Override
    public void preAllocate(int valueCount) {
      fv.allocateNew(valueCount);
    }
  }

  final class UUIDVectorizer implements Vectorizer<UUID> {
    private final FixedSizeBinaryVector fv;

    UUIDVectorizer(FixedSizeBinaryVector fsbv) {
      this.fv = fsbv;
    }

    @Override
    public void vectorize(int index, UUID value) {
      ByteBuffer buf = ByteBuffer.allocate(16);
      buf.order(ByteOrder.LITTLE_ENDIAN);
      buf.putLong(value.getLeastSignificantBits());
      buf.putLong(value.getMostSignificantBits());
      fv.set(index, buf.array());
    }

    @Override
    public Class<UUID> getType() {
      return UUID.class;
    }

    @Override
    public void preAllocate(int valueCount) {
      fv.allocateNew(valueCount);
    }
  }

  final class UnknownVectorizer implements Vectorizer<Object> {
    @Override
    public void vectorize(int index, Object value) {
      // TODO(ramesh): decide between throwing an error or logging a warning.

    }

    @Override
    public Class<Object> getType() {
      return Object.class;
    }

    @Override
    public void preAllocate(int valueCount) {}
  }
}
