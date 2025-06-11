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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.Translators;
import io.isima.bios.data.synthesis.generators.AttributeGeneratorsCreator;
import io.isima.bios.data.synthesis.generators.RecordGenerator;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ExportStatus;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

public class SignalTransformerExportTest {

  private static final long DAYS_IN_MILLIS = 24 * 3600 * 1000;
  private static final int S3_ITER_COUNT = 10;
  private static final String TEST_SIGNAL = "employeeDetails";
  private static final String TEST_TENANT = "tenant1";

  private static ObjectMapper objectMapper;

  private RecordGenerator recordGenerator;
  private SignalToArrowTransformer arrowTransformer;
  private SignalConfig employeeConfig;

  @BeforeClass
  public static void setUpBeforeClass() {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Before
  public void setUp() throws Exception {
    final String srcSimple =
        "{"
            + "  'signalName': '"
            + TEST_SIGNAL
            + "',"
            + "  'version': 1588175368661,"
            + "  'biosVersion': 1588175368660,"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {"
            + "      'attributeName': 'employeeName',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'employeeSalary',"
            + "      'type': 'Decimal'"
            + "    },"
            + "    {"
            + "      'attributeName': 'employeeDept',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'employeeAge',"
            + "      'type': 'Integer'"
            + "    }"
            + "  ]"
            + "}";

    employeeConfig = objectMapper.readValue(srcSimple.replace("'", "\""), SignalConfig.class);
    final var creator = AttributeGeneratorsCreator.getCreator(TEST_TENANT, employeeConfig);
    recordGenerator = new RecordGenerator(creator);

    StreamConfig stream = Translators.toTfos(employeeConfig);
    Assert.assertEquals(TEST_SIGNAL, stream.getName());
    arrowTransformer = new SignalToArrowTransformer(stream);
  }

  @Test
  public void testS3Config() {
    final var config = getExportConfig();
    final var config2 = getExportConfig();
    assertTrue(config.isConfigEquals(config2.getDelegate()));
    config2.getDelegate().setStatus(ExportStatus.ENABLED);
    assertTrue(config.isConfigEquals(config2.getDelegate()));
    final var config3 = new ExportDestinationConfig();
    config3.setStorageConfig(Map.of("s3Junk", "123"));
    assertFalse(config.isConfigEquals(config3));
  }

  @Test
  public void testFileBasedExporter() throws InterruptedException {
    final FileLoader exporter = new FileLoader();
    runTest(exporter);
    assertThat(exporter.getParquetPaths().size(), is(S3_ITER_COUNT));
    // TODO(ramesh): use parquet reader to assert data
    for (Path path : exporter.getParquetPaths()) {
      try {
        Files.deleteIfExists(path);
      } catch (IOException ignored) {
        System.out.println("File " + path.toString() + " not deleted");
      }
    }
  }

  @Ignore
  @Test
  public void testS3BasedExporter() throws InterruptedException {
    final var s3Loader = new ParquetToS3Loader(getExportConfig());
    runTest(s3Loader.getExporter(TEST_SIGNAL, employeeConfig.getVersion()));
  }

  private S3ExportConfig getExportConfig() {
    return S3ExportConfig.from(
        "testS3",
        "export-s3-bios-test",
        "testAccessKey",
        "testSecretAccessKey",
        Region.AP_SOUTH_1.toString());
  }

  private void runTest(DataExporter<ByteBuffer> exporter) throws InterruptedException {
    final var parquet =
        new ArrowToParquetTransformer(
            TEST_TENANT, TEST_SIGNAL, employeeConfig.getVersion(), arrowTransformer.getDataPipe());

    AtomicInteger done = new AtomicInteger(S3_ITER_COUNT);

    parquet.registerDataSink(
        (b, l) -> {
          exporter
              .exportDataAsync(b, l)
              .whenComplete(
                  (r, t) -> {
                    synchronized (this) {
                      this.notify();
                      done.decrementAndGet();
                    }
                    if (t != null) {
                      t.printStackTrace();
                      throw new RuntimeException(t);
                    }
                  });
        });

    for (int i = S3_ITER_COUNT; i > 0; i--) {
      final long daysBack = i * DAYS_IN_MILLIS;
      final var events1 = buildEvents(1000, daysBack);
      final var events2 = buildEvents(1000, daysBack);
      final var events3 = buildEvents(1000, daysBack);

      arrowTransformer.transformEvents(events1);
      arrowTransformer.transformEvents(events2);
      arrowTransformer.transformEvents(events3);

      arrowTransformer.getDataPipe().drainPipe();
      parquet.flushDataToSink();
      synchronized (this) {
        while (done.get() >= i) {
          this.wait();
        }
      }
      parquet.sinkFlushed();
      arrowTransformer.getDataPipe().clearDrain();
    }
  }

  private List<Event> buildEvents(int cnt, long delta) {
    final List<Event> eventList = new ArrayList<>();
    for (int i = 0; i < cnt; i++) {
      final var event = recordGenerator.generate();
      Instant inst = Instant.ofEpochMilli(System.currentTimeMillis() - delta);
      event.setIngestTimestamp(Date.from(inst));
      eventList.add(event);
    }
    return eventList;
  }

  private class FileLoader implements DataExporter<ByteBuffer> {
    private final List<Path> parquetPaths = new ArrayList<>();

    @Override
    public CompletableFuture<Void> exportDataAsync(ByteBuffer data, long timestamp) {
      return CompletableFuture.runAsync(() -> writeDataToFile(data, timestamp));
    }

    private void writeDataToFile(ByteBuffer data, long timestamp) {
      try {
        final var fName = Files.createTempFile(TEST_SIGNAL, String.valueOf(timestamp));
        boolean success = false;
        try (WritableByteChannel channel = Files.newByteChannel(fName, StandardOpenOption.APPEND)) {
          channel.write(data);
          success = true;
        } catch (IOException e) {
          Assert.fail(
              "failed to write local file " + fName.toString() + " error = " + e.getMessage());
        } finally {
          if (success) {
            parquetPaths.add(fName);
          }
        }
      } catch (IOException e) {
        Assert.fail("failed to create temp file " + " error = " + e.getMessage());
      }
    }

    private List<Path> getParquetPaths() {
      return parquetPaths;
    }
  }
}
