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

import java.io.IOException;
import java.io.OutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class ParquetIO {
  private static final int PARQUET_IO_BUF_SIZE = 16 * 1024;

  @FunctionalInterface
  public interface StreamCreator {
    OutputStream createStream(int bufSize, boolean overwrite) throws IOException;
  }

  public static OutputFile createFromSupplier(StreamCreator creator) {
    return new OutputFile() {
      @Override
      public PositionOutputStream create(long blockSizeHint) throws IOException {
        return createPositionOutputStream(creator.createStream(PARQUET_IO_BUF_SIZE, false));
      }

      @Override
      public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return createPositionOutputStream(creator.createStream(PARQUET_IO_BUF_SIZE, true));
      }

      @Override
      public boolean supportsBlockSize() {
        return false;
      }

      @Override
      public long defaultBlockSize() {
        return 0;
      }
    };
  }

  private static PositionOutputStream createPositionOutputStream(OutputStream output) {
    return new PositionOutputStream() {
      private long position = 0L;

      @Override
      public long getPos() throws IOException {
        return position;
      }

      @Override
      public void write(int b) throws IOException {
        output.write(b);
        position++;
      }

      @Override
      public void close() throws IOException {
        output.close();
      }
    };
  }
}
