/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.InputStream;
import java.util.Locale;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Enum representing a single file encryption algorithm in which the source
 * files may be compressed by.
 */
public enum MantaCompressionType {
    @JsonProperty("BZIP2")
    BZIP2("bz2", CompressorStreamFactory.BZIP2),
    @JsonProperty("GZIP")
    GZIP("gz", CompressorStreamFactory.GZIP),
    @JsonProperty("XZ")
    XZ("xz", CompressorStreamFactory.XZ);

    private static final Map<String, MantaCompressionType> EXTENSION_LOOKUP;

    static final CompressorStreamFactory COMPRESSOR_STREAM_FACTORY =
        new CompressorStreamFactory();

    static {
        ImmutableMap.Builder<String, MantaCompressionType> extensionMapBuilder =
                new ImmutableMap.Builder<>();

        for (MantaCompressionType type : values()) {
            extensionMapBuilder.put(type.fileExtension, type);
        }
        EXTENSION_LOOKUP = extensionMapBuilder.build();
    }

    private final String fileExtension;
    private final String compressorName;

    MantaCompressionType(final String fileExtension, final String compressorName) {
        this.fileExtension = fileExtension;
        this.compressorName = compressorName;
    }

    public static MantaCompressionType valueOfExtension(final String extension) {
        return EXTENSION_LOOKUP.get(extension.toLowerCase(Locale.ENGLISH));
    }

    public static boolean isExtensionSupported(final String extension) {
        return EXTENSION_LOOKUP.containsKey(extension.toLowerCase(Locale.ENGLISH));
    }

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

    public CompressorInputStream createStream(final InputStream in) {
        final String algorithm = compressorName;

        try {
            return COMPRESSOR_STREAM_FACTORY.createCompressorInputStream(
                    algorithm, in);
        } catch (CompressorException e) {
            String msg = "Unable to create decompression stream";
            MantaPrestoRuntimeException me = new MantaPrestoRuntimeException(msg, e);
            me.setContextValue("compressionAlgorithm", algorithm);
            throw me;
        }
    }
}
