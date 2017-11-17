/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.compress.snappy;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Determines if Snappy native library is available and loads it if available.
 */
public class LoadSnappy {
    private static final Logger LOG = LoggerFactory.getLogger(LoadSnappy.class);

    private static boolean LOADED = false;

    static {
        try {
            System.loadLibrary("snappy");
            System.loadLibrary("hadoopsnappy");

            // Find the path to the native library
            String snappyPath = findLibrary("snappy");

            // Initialize the native library. This causes hadoopsnappy to
            // dynamically load the symbols from the native snappy library.
            // If this fails, the library can't be used, and attempting to
            // use it will cause the JVM to crash.
            SnappyDecompressor.initIDs(snappyPath);

            LOADED = true;
        } catch (UnsatisfiedLinkError ex) {
            LOG.warn("Failed to load library from " +
                    System.getProperty("java.library.path") + ": " + ex.getMessage() + "\n" +
                    "Libraries loaded:\n" + Joiner.on("\n").join(allJniLibraries()));
        }
        if (LOADED) {
            LOG.info("Snappy native library loaded");
        } else {
            LOG.warn("Snappy native library not loaded");
        }
    }

    private static String findLibrary(String name) {
        name = System.mapLibraryName(name).replace(".jnilib", ".dylib");
        String paths[] = System.getProperty("java.library.path").split(":");
        for (String path : paths) {
            File file = new File(path, name);
            if (file.exists()) {
                return file.toString();
            }
        }
        throw new UnsatisfiedLinkError("cannot find path to " + name);
    }

    private static List<String> allJniLibraries() {
        try {
            java.lang.reflect.Field loadedLibraryNames =
                    ClassLoader.class.getDeclaredField("loadedLibraryNames");
            loadedLibraryNames.setAccessible(true);
            @SuppressWarnings("unchecked") final Vector<String> libraries = (Vector<String>) loadedLibraryNames.get(ClassLoader.getSystemClassLoader());

            return new ArrayList<>(libraries);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns if Snappy native library is loaded.
     *
     * @return <code>true</code> if Snappy native library is loaded,
     * <code>false</code> if not.
     */
    public static boolean isLoaded() {
        return LOADED;
    }

}
