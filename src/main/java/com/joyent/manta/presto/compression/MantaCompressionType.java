/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.compression;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.Files;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.CompressorStreamProvider;

import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;

import static java.util.Objects.requireNonNull;

/**
 * Enum representing a single file encryption algorithm in which the source
 * files may be compressed by.
 *
 * @since 1.0.0
 */
public enum MantaCompressionType {
    /**
     * Bzip2 algorithm.
     */
    @JsonProperty("BZIP2")
    BZIP2("bz2", CompressorStreamFactory.BZIP2),
    /**
     * Gzip/deflate algorithm.
     */
    @JsonProperty("GZIP")
    GZIP("gz", CompressorStreamFactory.GZIP),
    /**
     * LZ4 raw algorithm.
     */
    LZ4("lz4", CompressorStreamFactory.LZ4_BLOCK),
    /**
     * Snappy raw algorithm implemented by the Xerial native library.
     */
    @JsonProperty("XERIAL_SNAPPY")
    XERIAL_SNAPPY("xsnappy", SnappyStreamProvider.XERIAL_SNAPPY_RAW),
    /**
     * Snappy raw algorithm implemented by the Hadoop native library.
     */
    @JsonProperty("HADOOP_SNAPPY")
    HADOOP_SNAPPY("snappy", SnappyStreamProvider.HADOOP_SNAPPY_RAW),
    /**
     * XZ algorithm.
     */
    @JsonProperty("XZ")
    XZ("xz", CompressorStreamFactory.XZ);

    /**
     * Look up table for mapping file extension to compression algorithm.
     */
    private static final Map<String, MantaCompressionType> EXTENSION_LOOKUP;

    /**
     * Singleton static instance of compressor stream providers that can be
     * safely reused.
     */
    public static final SortedMap<String, CompressorStreamProvider> COMPRESSOR_STREAM_PROVIDERS;

    static {
        ImmutableMap.Builder<String, MantaCompressionType> extensionMapBuilder =
                new ImmutableMap.Builder<>();

        for (MantaCompressionType type : values()) {
            extensionMapBuilder.put(type.fileExtension, type);
        }
        EXTENSION_LOOKUP = extensionMapBuilder.build();

        /* We manually build a provider lookup because the SPI service loader
         * has problems working properly when running within Presto. */

        ImmutableSortedMap.Builder<String, CompressorStreamProvider> builder =
                new ImmutableSortedMap.Builder<>(String.CASE_INSENSITIVE_ORDER);

        SnappyStreamProvider snappyStreamProvider = new SnappyStreamProvider();

        SortedMap<String, CompressorStreamProvider> defaultProviders =
                new CompressorStreamFactory().getCompressorInputStreamProviders();

        builder.putAll(defaultProviders);

        if (!defaultProviders.containsKey(SnappyStreamProvider.XERIAL_SNAPPY_RAW)) {
            builder.put(SnappyStreamProvider.XERIAL_SNAPPY_RAW, snappyStreamProvider);
        }

        if (!defaultProviders.containsKey(SnappyStreamProvider.HADOOP_SNAPPY_RAW)) {
            builder.put(SnappyStreamProvider.HADOOP_SNAPPY_RAW, snappyStreamProvider);
        }

        COMPRESSOR_STREAM_PROVIDERS = builder.build();
    }

    /**
     * File extension associated with the compression algorithm.
     */
    private final String fileExtension;

    /**
     * Compressor name used to identify the algorithm with Commons Compress.
     */
    private final String compressorName;

    /**
     * Creates a new enum instance based on the passed parameters.
     *
     * @param fileExtension file extension associated with the compression algorithm
     * @param compressorName compressor name used to identify the algorithm with Commons Compress
     */
    MantaCompressionType(final String fileExtension, final String compressorName) {
        this.fileExtension = fileExtension;
        this.compressorName = compressorName;
    }

    /**
     * Looks up and returns a {@link MantaCompressionType} based on the passed
     * filename extension.
     *
     * @param extension filename extension
     * @return {@link MantaCompressionType} instance if matched otherwise null
     */
    public static MantaCompressionType valueOfExtension(final String extension) {
        return EXTENSION_LOOKUP.get(extension.toLowerCase(Locale.ENGLISH));
    }

    /**
     * Checks to see if the given file extension is associated with a supported
     * compression algorithm.
     *
     * @param extension file extension to check without preceding dot
     * @return true if there is a supported algorithm otherwise false
     */
    public static boolean isExtensionSupported(final String extension) {
        return EXTENSION_LOOKUP.containsKey(extension.toLowerCase(Locale.ENGLISH));
    }

    /**
     * Helper method that wraps a passed Manta object input stream object with
     * a decompressing input stream if the source object has an extension
     * associated with a supported compression algorithm.
     *
     * @param mantaInputStream input stream to wrap
     * @return wrapped stream or passed stream
     */
    public static InputStream wrapMantaStreamIfCompressed(final MantaObjectInputStream mantaInputStream) {
        requireNonNull(mantaInputStream, "Manta input stream is null");

        String fileExtension = Files.getFileExtension(mantaInputStream.getPath());

        if (isExtensionSupported(fileExtension)) {
            MantaCompressionType compressionType = valueOfExtension(fileExtension);
            return compressionType.createStream(mantaInputStream);
        } else {
            return mantaInputStream;
        }
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public String getCompressorName() {
        return compressorName;
    }

    /**
     * Creates a new decompression stream that wraps the passed stream using
     * the algorithm associated with the current instance.
     *
     * @param in input stream
     * @return decompression stream associated with this instance
     */
    public CompressorInputStream createStream(final InputStream in) {
        requireNonNull(in, "InputStream to decompress is null");
        final String algorithm = compressorName;

        final CompressorStreamProvider provider = COMPRESSOR_STREAM_PROVIDERS.get(
                algorithm);

        if (provider == null) {
            String msg = "No compressor provider matches the passed string";
            MantaPrestoRuntimeException e = new MantaPrestoRuntimeException(msg);
            e.setContextValue("compressorName", algorithm);
            throw e;
        }

        try {
            return provider.createCompressorInputStream(algorithm, in, true);
        } catch (CompressorException e) {
            String msg = "Unable to create decompression stream";
            MantaPrestoRuntimeException me = new MantaPrestoRuntimeException(msg, e);
            me.setContextValue("compressionAlgorithm", algorithm);
            throw me;
        }
    }
}
