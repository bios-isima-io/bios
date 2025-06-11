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
package io.isima.bios.data.impl.sketch;

import static io.isima.bios.models.v1.InternalAttributeType.ENUM;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.impl.models.FunctionResult;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import io.isima.bios.models.v1.InternalAttributeType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.DeserializeResult;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.datasketches.tuple.SummaryFactory;
import org.apache.datasketches.tuple.SummarySetOperations;
import org.apache.datasketches.tuple.Union;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SketchSampleCounts extends DataSketch {
  static final Logger logger = LoggerFactory.getLogger(SketchSampleCounts.class);
  public static final String ATTRIBUTE_NAME_SAMPLE = "_sample";
  public static final String ATTRIBUTE_NAME_SAMPLE_COUNT = "_sampleCount";
  public static final String ATTRIBUTE_NAME_SAMPLE_LENGTH = "_sampleLength";
  public static final String STRING_SAMPLE_SIZE_KEY = "prop.stringSampleSize";
  private static final int STRING_SAMPLE_SIZE_DEFAULT = 512;
  private static final int LG_K = 8;
  private static final float SAMPLING_PROBABILITY = 1.0F;
  public static final int INVALID_LENGTH = -1;

  @Getter private long count;

  /** This is an updatable sketch, typically created when creating new sketches from scratch. */
  @SuppressWarnings("rawtypes")
  private final CountTupleSketch countTupleSketch;

  /** This is a readonly sketch, typically created when merging multiple existing sketches. */
  @SuppressWarnings("rawtypes")
  private final CompactSketch countTupleSketchCompact;

  public SketchSampleCounts(InternalAttributeType attributeType, byte[] data, long count) {
    super(attributeType, DataSketchType.SAMPLE_COUNTS);
    countTupleSketchCompact = null;
    if (data != null) {
      this.count = count;
      final var memory = Memory.wrap(data);
      switch (attributeType) {
        case LONG:
          countTupleSketch = new CountTupleSketch<Long>(memory);
          break;
        case DOUBLE:
          countTupleSketch = new CountTupleSketch<Double>(memory);
          break;
        case ENUM:
          countTupleSketch = new CountTupleSketch<Integer>(memory);
          break;
        case STRING:
          countTupleSketch = new CountTupleSketch<String>(memory);
          break;
        default:
          throw new UnsupportedOperationException(attributeType.toString());
      }
    } else {
      this.count = 0;
      switch (attributeType) {
        case LONG:
          countTupleSketch = new CountTupleSketch<Long>();
          break;
        case DOUBLE:
          countTupleSketch = new CountTupleSketch<Double>();
          break;
        case ENUM:
          countTupleSketch = new CountTupleSketch<Integer>();
          break;
        case STRING:
          countTupleSketch = new CountTupleSketch<String>();
          break;
        default:
          throw new UnsupportedOperationException(attributeType.toString());
      }
    }
  }

  @SuppressWarnings("unchecked")
  public SketchSampleCounts(
      InternalAttributeType attributeType, Collection<DataSketch> sketchesToMerge) {
    super(attributeType, DataSketchType.SAMPLE_COUNTS);
    countTupleSketch = null;
    count = 0;
    switch (attributeType) {
      case LONG:
        final Union<CountSummary<Long>> unionLong =
            new Union<>(1 << LG_K, new CountSummarySetOperations<Long>());
        for (final var sketch : sketchesToMerge) {
          unionLong.update(((SketchSampleCounts) sketch).countTupleSketch);
          count += sketch.getCount();
        }
        countTupleSketchCompact = unionLong.getResult();
        break;
      case DOUBLE:
        final Union<CountSummary<Double>> unionDouble =
            new Union<>(1 << LG_K, new CountSummarySetOperations<Double>());
        for (final var sketch : sketchesToMerge) {
          unionDouble.update(((SketchSampleCounts) sketch).countTupleSketch);
          count += sketch.getCount();
        }
        countTupleSketchCompact = unionDouble.getResult();
        break;
      case ENUM:
        final Union<CountSummary<Integer>> unionInteger =
            new Union<>(1 << LG_K, new CountSummarySetOperations<Integer>());
        for (final var sketch : sketchesToMerge) {
          unionInteger.update(((SketchSampleCounts) sketch).countTupleSketch);
          count += sketch.getCount();
        }
        countTupleSketchCompact = unionInteger.getResult();
        break;
      case STRING:
        final Union<CountSummary<String>> unionString =
            new Union<>(1 << LG_K, new CountSummarySetOperations<String>());
        for (final var sketch : sketchesToMerge) {
          unionString.update(((SketchSampleCounts) sketch).countTupleSketch);
          count += sketch.getCount();
        }
        countTupleSketchCompact = unionString.getResult();
        break;
      default:
        throw new UnsupportedOperationException(attributeType.toString());
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public FunctionResult evaluate(
      MetricFunction function, AttributeDesc attributeDesc, ExecutionState state) {
    assert (function.getDataSketchType() == sketchType);
    assert (count > 0);
    @SuppressWarnings("rawtypes")
    final Sketch sketch;
    if (countTupleSketch != null) {
      sketch = countTupleSketch;
    } else {
      sketch = countTupleSketchCompact;
    }
    assert (sketch != null);

    switch (function) {
      case NUMSAMPLES:
        return new FunctionResult((long) sketch.getRetainedEntries());
      case SAMPLINGFRACTION:
        return new FunctionResult(sketch.getTheta());
      case SAMPLECOUNTS:
        final List<SampleCountRow> rows = new ArrayList<>();
        final var sampleIterator = sketch.iterator();
        while (sampleIterator.next()) {
          final var summary = sampleIterator.getSummary();
          switch (attributeType) {
            case LONG:
              CountSummary<Long> summaryLong = (CountSummary<Long>) summary;
              rows.add(
                  new SampleCountRow(
                      summaryLong.getPossiblyTruncatedValue(), summaryLong.getCount(), null));
              break;
            case DOUBLE:
              CountSummary<Double> summaryDouble = (CountSummary<Double>) summary;
              rows.add(
                  new SampleCountRow(
                      summaryDouble.getPossiblyTruncatedValue(), summaryDouble.getCount(), null));
              break;
            case ENUM:
              CountSummary<Integer> summaryInteger = (CountSummary<Integer>) summary;
              final int enumValueInt = summaryInteger.getPossiblyTruncatedValue();
              final String enumValueString =
                  (String)
                      Attributes.dataEngineToPlane(
                          enumValueInt,
                          attributeDesc,
                          state.getTenantName(),
                          state.getStreamName());
              rows.add(
                  new SampleCountRow(
                      enumValueString, summaryInteger.getCount(), (long) enumValueString.length()));
              break;
            case STRING:
              CountSummary<String> summaryString = (CountSummary<String>) summary;
              rows.add(
                  new SampleCountRow(
                      summaryString.getPossiblyTruncatedValue(),
                      summaryString.getCount(),
                      (long) summaryString.getFullValueLength()));
              break;
            default:
              throw new UnsupportedOperationException(attributeType.toString());
          }
        }
        // Sort the output as: 1. Decreasing by count, 2. Increasing by sample.
        rows.sort(new SampleCountRowComparator());
        final FunctionResult functionResult;
        if ((attributeType == ENUM) || (attributeType == STRING)) {
          functionResult =
              new FunctionResult(
                  List.of(
                      ATTRIBUTE_NAME_SAMPLE,
                      ATTRIBUTE_NAME_SAMPLE_COUNT,
                      ATTRIBUTE_NAME_SAMPLE_LENGTH));
        } else {
          functionResult =
              new FunctionResult(List.of(ATTRIBUTE_NAME_SAMPLE, ATTRIBUTE_NAME_SAMPLE_COUNT));
        }
        final var finalResultRows = functionResult.getComplexValue().getRows();
        for (final var row : rows) {
          if (row.getLength() != null) {
            finalResultRows.add(List.of(row.getSample(), row.getCount(), row.getLength()));
          } else {
            finalResultRows.add(List.of(row.getSample(), row.getCount()));
          }
        }
        return functionResult;
      default:
        throw new UnsupportedOperationException(function.toString());
    }
  }

  @Getter
  @RequiredArgsConstructor
  private static class SampleCountRow {
    private final Object sample;
    private final long count;
    private final Long length;
  }

  private static class SampleCountRowComparator implements Comparator<SampleCountRow> {
    @Override
    public int compare(SampleCountRow row1, SampleCountRow row2) {
      if (row1.count != row2.count) {
        return Long.compare(row2.getCount(), row1.getCount());
      }
      final Object sample1 = row1.getSample();
      if (sample1 instanceof Long) {
        return Long.compare((Long) row1.getSample(), (Long) row2.getSample());
      } else if (sample1 instanceof Double) {
        return Double.compare((Double) row1.getSample(), (Double) row2.getSample());
      } else if (sample1 instanceof String) {
        return ((String) row1.getSample()).compareTo((String) row2.getSample());
      } else {
        throw new UnsupportedOperationException(sample1.toString());
      }
    }
  }

  @Override
  public ByteBuffer getData() {
    final ByteBuffer buffer = ByteBuffer.wrap(countTupleSketch.toByteArray());
    // TODO: compact before serializing?
    // final ByteBuffer buffer = ByteBuffer.wrap(countTupleSketch.compact().toByteArray());
    return buffer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void update(Object attributeValue) {
    if (attributeValue == null) {
      return;
    }
    countTupleSketch.update(attributeValue);
    count++;
    return;
  }

  @Override
  public void populateSummary(SketchSummary sketchSummary) {
    assert (countTupleSketch != null);
    sketchSummary.setCount(count);
    sketchSummary.setNumSamples((long) countTupleSketch.getRetainedEntries());
    sketchSummary.setSamplingFraction(countTupleSketch.getTheta());
  }

  static class Actor extends SketchActor {
    public Actor() {
      super(DataSketchType.SAMPLE_COUNTS);
    }

    @Override
    public Set<SketchSummaryColumn> columnsNeededForFunction(MetricFunction function) {
      assert (function.getDataSketchType() == myDataSketchType);
      final var columns = new HashSet<SketchSummaryColumn>();
      if (function == MetricFunction.SAMPLECOUNTS) {
        columns.add(SketchSummaryColumn.NUMSAMPLES);
      } else {
        columns.add(SketchSummaryColumn.valueOf(function.name()));
      }
      return columns;
    }

    @Override
    protected FunctionResult getResultWithNoDataPoints(MetricFunction function) {
      switch (function) {
        case NUMSAMPLES:
          return new FunctionResult(0L);
        case SAMPLECOUNTS:
          // We need data points in order to have any samples. Without any data points
          // we cannot return meaningful results for samples.
          return null;
        default:
          return new FunctionResult(Double.NaN);
      }
    }
  }

  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @NoArgsConstructor
  @Getter
  @EqualsAndHashCode
  private static class CountSummary<U> implements UpdatableSummary<U> {
    /** The length of the original value if it is a String; -1 otherwise. */
    private int fullValueLength = INVALID_LENGTH;

    @Setter private long count = 0;
    private U possiblyTruncatedValue = null;

    @SuppressWarnings("unchecked")
    @Override
    public void update(U value) {
      int stringSampleSize =
          SharedProperties.getCached(STRING_SAMPLE_SIZE_KEY, STRING_SAMPLE_SIZE_DEFAULT);
      count++;
      if (possiblyTruncatedValue == null) {
        if (value instanceof String) {
          fullValueLength = ((String) value).length();
          if (fullValueLength > stringSampleSize) {
            possiblyTruncatedValue = (U) ((String) value).substring(0, stringSampleSize);
          } else {
            possiblyTruncatedValue = value;
          }
        } else {
          possiblyTruncatedValue = value;
        }
      }
    }

    @Override
    public CountSummary<U> copy() {
      return new CountSummary<U>(fullValueLength, count, possiblyTruncatedValue);
    }

    private static final int SERIALIZED_SIZE_NUMBERS = Long.BYTES;

    /**
     * Serializes this summary object to a byte array. Layout is: Integer: total number of bytes of
     * the serialized byte array Long: count Byte: InternalAttributeType proxy
     * InternalAttributeType-specific part: STRING: Integer: length of the full value Integer:
     * length of the serialized possiblyTruncatedValue byte[]: possiblyTruncatedValue in byte array
     * form LONG: Long DOUBLE: Double ENUM: Long (int cast to long)
     */
    @Override
    public byte[] toByteArray() {
      assert (possiblyTruncatedValue != null);
      final int storedValueSize;
      final byte[] stringInBytes;

      if (possiblyTruncatedValue instanceof String) {
        stringInBytes = ((String) possiblyTruncatedValue).getBytes(UTF_8);
        storedValueSize = Integer.BYTES * 2 + stringInBytes.length;
      } else {
        stringInBytes = null;
        storedValueSize = SERIALIZED_SIZE_NUMBERS;
      }
      final int totBytes = Integer.BYTES + Long.BYTES + Byte.BYTES + storedValueSize;

      final byte[] out = new byte[totBytes];
      final WritableMemory wmem = WritableMemory.wrap(out);
      final WritableBuffer wbuf = wmem.asWritableBuffer();

      wbuf.putInt(totBytes);
      wbuf.putLong(count);
      if (possiblyTruncatedValue instanceof Long) {
        wbuf.putByte(InternalAttributeType.LONG.getProxy());
        wbuf.putLong((Long) possiblyTruncatedValue);
      } else if (possiblyTruncatedValue instanceof Double) {
        wbuf.putByte(InternalAttributeType.DOUBLE.getProxy());
        wbuf.putDouble((Double) possiblyTruncatedValue);
      } else if (possiblyTruncatedValue instanceof Integer) {
        wbuf.putByte(ENUM.getProxy());
        wbuf.putLong((Integer) possiblyTruncatedValue);
      } else if (possiblyTruncatedValue instanceof String) {
        assert stringInBytes != null;
        wbuf.putByte(STRING.getProxy());
        wbuf.putInt(fullValueLength);
        wbuf.putInt(stringInBytes.length);
        wbuf.putByteArray(stringInBytes, 0, stringInBytes.length);
      } else {
        throw new UnsupportedOperationException(possiblyTruncatedValue.toString());
      }
      assert wbuf.getPosition() == totBytes;
      return out;
    }

    @SuppressWarnings("unchecked")
    public CountSummary(final Memory mem) {
      final Buffer buf = mem.asBuffer();
      final int totalBytes = buf.getInt();
      checkInBytes(mem, totalBytes);
      this.count = buf.getLong();
      final var attributeTypeProxy = buf.getByte();
      final var attributeType = InternalAttributeType.fromProxy(attributeTypeProxy);
      switch (attributeType) {
        case LONG:
          this.possiblyTruncatedValue = (U) (Long) buf.getLong();
          break;
        case DOUBLE:
          this.possiblyTruncatedValue = (U) (Double) buf.getDouble();
          break;
        case ENUM:
          this.possiblyTruncatedValue = (U) (Integer) (int) buf.getLong();
          break;
        case STRING:
          this.fullValueLength = buf.getInt();
          final int stringInBytesLen = buf.getInt();
          final byte[] byteArr = new byte[stringInBytesLen];
          buf.getByteArray(byteArr, 0, stringInBytesLen);
          this.possiblyTruncatedValue = (U) new String(byteArr, UTF_8);
          break;
        default:
          throw new UnsupportedOperationException(attributeType.toString());
      }
    }

    static void checkInBytes(final Memory mem, final int totBytes) {
      if (mem.getCapacity() < totBytes) {
        throw new SketchesArgumentException("Incoming Memory has insufficient capacity.");
      }
    }
  }

  private static class CountSummaryDeserializer<U> implements SummaryDeserializer<CountSummary<U>> {

    @Override
    public DeserializeResult<CountSummary<U>> heapifySummary(final Memory mem) {
      final CountSummary<U> summary = new CountSummary<U>(mem);
      final int totBytes = mem.getInt(0);
      return new DeserializeResult<>(summary, totBytes);
    }
  }

  private static class CountSummaryFactory<U> implements SummaryFactory<CountSummary<U>> {

    @Override
    public CountSummary<U> newSummary() {
      return new CountSummary<U>();
    }
  }

  private static class CountSummarySetOperations<U>
      implements SummarySetOperations<CountSummary<U>> {

    @Override
    public CountSummary<U> union(final CountSummary<U> a, final CountSummary<U> b) {
      final CountSummary<U> union = a.copy();
      union.setCount(a.getCount() + b.getCount());
      return union;
    }

    @Override
    public CountSummary<U> intersection(final CountSummary<U> a, final CountSummary<U> b) {
      throw new UnsupportedOperationException();
    }
  }

  private static class CountTupleSketch<U> extends UpdatableSketch<U, CountSummary<U>> {

    public CountTupleSketch() {
      super(1 << LG_K, ResizeFactor.X8.lg(), SAMPLING_PROBABILITY, new CountSummaryFactory<U>());
    }

    public CountTupleSketch(final Memory mem) {
      super(mem, new CountSummaryDeserializer<U>(), new CountSummaryFactory<U>());
    }

    public void update(final U value) {
      if (value instanceof Long) {
        super.update((Long) value, value);
      } else if (value instanceof Double) {
        super.update((Double) value, value);
      } else if (value instanceof Integer) {
        super.update((Integer) value, value);
      } else if (value instanceof String) {
        super.update((String) value, value);
      } else {
        throw new UnsupportedOperationException(value.toString());
      }
    }
  }
}
