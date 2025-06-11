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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Loads the jni library connecting with the arrow c++ library that uses an efficient mechanism to
 * convert arrow into parquet.
 */
public class ParquetJniLoader {
  private static final String PARQUET_LIBRARY_NAME = "parquetjni";
  private static final String PARQUET_LIBRARY_LOCATION = "NATIVE";
  private static final String PLATFORM = System.getProperty("os.arch");

  private static final AtomicReference<ParquetJniLoader> INSTANCE = new AtomicReference<>();

  static ParquetJniLoader getInstance() {
    if (INSTANCE.get() == null) {
      final var newLoader = new ParquetJniLoader();
      newLoader.loadParquetJniLibraryFromJar();
      INSTANCE.compareAndSet(null, newLoader);
    }

    return INSTANCE.get();
  }

  private ParquetJniLoader() {}

  private void loadParquetJniLibraryFromJar() {
    try {
      final String libraryName = System.mapLibraryName(PARQUET_LIBRARY_NAME);

      final File tmpFile = moveLibFromJarToTemp(libraryName);
      System.load(tmpFile.getAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private File moveLibFromJarToTemp(String libraryName) throws IOException {
    final String srcLibrary =
        PARQUET_LIBRARY_LOCATION + File.separator + PLATFORM + File.separator + libraryName;
    final var tokens = libraryName.split("\\.");
    final String prefix;
    final String suffix;
    if (tokens.length == 1) {
      prefix = tokens[0];
      suffix = "";
    } else {
      prefix =
          Arrays.asList(tokens).subList(0, tokens.length - 1).stream()
              .collect(Collectors.joining("."));
      suffix = "." + tokens[tokens.length - 1];
    }
    final File targetLibrary = File.createTempFile(prefix, suffix);
    targetLibrary.deleteOnExit();
    try (final InputStream inputStream =
        ParquetJniLoader.class.getClassLoader().getResourceAsStream(srcLibrary)) {
      if (inputStream != null) {
        Files.copy(inputStream, targetLibrary.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return targetLibrary;
      }
    }
    throw new FileNotFoundException(srcLibrary);
  }
}
