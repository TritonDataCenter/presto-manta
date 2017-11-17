/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.compression;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamProvider;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.io.compress.snappy.LoadSnappy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Apache Compress compatible provider class that provides multiple Snappy
 * compatible implementations.
 *
 * @since 1.0.0
 */
public class SnappyStreamProvider implements CompressorStreamProvider {
    /**
     * Constant (value {@value}) used to identify the Xerial Snappy compression
     * algorithm.
     *
     * @since 1.0.0
     */
    public static final String XERIAL_SNAPPY_RAW = "XERIAL_SNAPPY_RAW";

    /**
     * Constant (value {@value}) used to identify the Hadoop Snappy compression
     * algorithm.
     *
     * @since 1.0.0
     */
    public static final String HADOOP_SNAPPY_RAW = "SNAPPY_HADOOP_RAW";

    private static final Set<String> COMPRESSOR_NAMES;

    static {
        Set<String> compressors = new HashSet<>(2);
        compressors.add(XERIAL_SNAPPY_RAW);
        compressors.add(HADOOP_SNAPPY_RAW);

        COMPRESSOR_NAMES = Collections.unmodifiableSet(compressors);
    }

    @Override
    public CompressorInputStream createCompressorInputStream(final String name,
                                                             final InputStream in,
                                                             final boolean decompressUntilEOF)
            throws CompressorException {
        if (name == null) {
            throw new IllegalArgumentException("Compressor name is null");
        }
        if (in == null) {
            throw new IllegalArgumentException("Input stream to wrap is null");
        }

        switch (name) {
            case XERIAL_SNAPPY_RAW:
                try {
                    return new XerialSnappyCompressorInputStream(in);
                } catch (IOException e) {
                    String msg = String.format("Error creating compressor stream for [%s]",
                            name);
                    throw new CompressorException(msg, e);
                }
            case HADOOP_SNAPPY_RAW:
                if (!LoadSnappy.isLoaded()) {
                    String msg = String.format("Hadoop Snappy native libraries weren't loaded. "
                                    + "Make sure they are installed in the java.library.path [%s].",
                            System.getProperty("java.library.path"));
                    throw new UnsupportedOperationException(msg);
                }

                return new HadoopSnappyCompressorInputStream(in);
            default:
                throw new CompressorException("Unknown compressor type: " + name);
        }
    }

    @Override
    public CompressorOutputStream createCompressorOutputStream(final String name,
                                                               final OutputStream out)
            throws CompressorException {
        throw new NotImplementedException("Compression has not been implemented");
    }

    @Override
    public Set<String> getInputStreamCompressorNames() {
        return COMPRESSOR_NAMES;
    }

    @Override
    public Set<String> getOutputStreamCompressorNames() {
        return Collections.emptySet();
    }
}
