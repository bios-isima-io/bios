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
package io.isima.bios.sdk.csdk;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

/**
 * Class that is usedful to load a shared library embedded in a jar file.
 *
 * <p>As a prerequisite, the target shared library file should be placed in directory
 * /NATIVE/${os.arch}/${os.name}/ under one of specified class path. The file can be in the a jar
 * file so that we can embed JNI shared library file in the package jar. Parameters ${os.arch} and
 * ${os.name} are values of system properties "os.arch" and "os.name" respectively. The method
 * {@link #getCurrentPlatformIdentifier()} provides the platform's identifier which is
 * ${os.arch}/${os.name} the loader would use. The application builder may call the method to
 * determine the directory to place the shared library file.
 *
 * <p>The loading method extracts the library file to a temporary file directory of the
 * application's platform. Then the loader loads the temporary file, and deletes it on exit.
 *
 * <p>If the JVM dumps core because of a bug in the library, find the file at
 * ${tempdir}/&lt;library_name&gt;.&lt;the_extension&gt;
 */
public class EmbeddedLibLoader {

  private EmbeddedLibLoader() {}

  /**
   * Loads a shared library.
   *
   * @param lib Library name
   * @return True if the library was successfully loaded, false otherwise.
   */
  public static boolean load(String lib) {
    boolean usingEmbedded = false;
    // attempt to locate embedded native library within JAR at following location:
    // /NATIVE/${os.arch}/${os.name}/${lib}.[so|dylib|dll]
    String[] allowedExtensions = new String[] {"so", "dylib", "dll"};
    StringBuilder url = new StringBuilder();
    url.append("/NATIVE/");
    url.append(getCurrentPlatformIdentifier()).append("/");
    URL nativeLibraryUrl = null;
    // loop through extensions, stopping after finding first one
    String extension = null;
    for (String ext : allowedExtensions) {
      nativeLibraryUrl = EmbeddedLibLoader.class.getResource(url.toString() + lib + "." + ext);
      if (nativeLibraryUrl != null) {
        extension = ext;
        break;
      }
    }

    if (nativeLibraryUrl != null) {
      // native library found within JAR, extract and load
      final File libfile;
      try {
        libfile = File.createTempFile(lib, "." + extension);
        libfile.deleteOnExit();
      } catch (IOException ex) {
        throw new RuntimeException("Failed to create temp file for shared library", ex);
      }

      try {
        try (final InputStream in = nativeLibraryUrl.openStream()) {
          try (final OutputStream out = new BufferedOutputStream(new FileOutputStream(libfile))) {
            int len = 0;
            byte[] buffer = new byte[8192];
            while ((len = in.read(buffer)) > -1) {
              out.write(buffer, 0, len);
            }
          }
        }
        System.load(libfile.getAbsolutePath());
        usingEmbedded = true;
      } catch (IOException x) {
        // mission failed, do nothing
      }
    }
    return usingEmbedded;
  }

  /**
   * Method to get the platform identifier the load uses.
   *
   * @return Platform identifier. The format of the identifier is <tt>"${os.arch}/${os.name}"</tt>
   *     but the os.name would be modified as follows:
   *     <ul>
   *       <li>Windows: Always "Windows" regardless the version
   *       <li>Mac OS X: Alwqays "Mac OS X" regardless the version Other OS: White spaces would be
   *           replaced by underscores.
   *     </ul>
   */
  public static String getCurrentPlatformIdentifier() {
    String osName = System.getProperty("os.name");
    if (osName.toLowerCase().contains("windows")) {
      osName = "Windows";
    } else if (osName.toLowerCase().contains("mac os x")) {
      osName = "Mac OS X";
    } else {
      osName = osName.replaceAll("\\s+", "_");
    }
    return System.getProperty("os.arch") + "/" + osName;
  }
}
