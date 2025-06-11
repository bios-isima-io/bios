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
package io.isima.bios.models;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExportDestinationConfigTest {
  private static final String S3_BUCKET_1 = "https://aws.amazon.com/s3/bucket1";
  private static final String S3_BUCKET_KEY = "s3BucketName";

  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {
    final String src =
        "{"
            + "  'exportDestinationName': 'awsS3Bucket1',"
            + "  'storageType': 'S3',"
            + "  'status' : 'Enabled',"
            + "  'storageConfig': {"
            + "'"
            + S3_BUCKET_KEY
            + "': '"
            + S3_BUCKET_1
            + "',"
            + "     's3AccessId': '10028XRTR77765',"
            + "     's3SecretKey': '1288337373737373773737356w5w6w56w6w'"
            + "  }"
            + "}";

    final ExportDestinationConfig s3Storage =
        objectMapper.readValue(src.replace("'", "\""), ExportDestinationConfig.class);
    assertThat(s3Storage.getStorageConfig().get(S3_BUCKET_KEY), is(S3_BUCKET_1));
    assertThat(s3Storage.getExportDestinationName(), is("awsS3Bucket1"));
    assertThat(s3Storage.getStatus(), is(ExportStatus.ENABLED));

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");
    assertEquals(expected, objectMapper.writeValueAsString(s3Storage));
  }
}
