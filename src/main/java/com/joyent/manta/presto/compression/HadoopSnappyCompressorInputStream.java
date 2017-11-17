/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.compression;

import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;

import java.io.IOException;
import java.io.InputStream;

/**
 * Apache Compress compatible {@link CompressorInputStream} that wraps the
 * Hadoop Snappy decompression libraries so that we get a sane stream that
 * works with the Apache libraries.
 *
 * @since 1.0.0
 */
public class HadoopSnappyCompressorInputStream extends CompressorInputStream {
    /** Snappy buffer size to use for decompression. */
    public static final int SNAPPY_BUFFER_SIZE_DEFAULT = 256 * 1024;

    private final BlockDecompressorStream decompressorStream;

    /**
     * Creates a new instance that decompresses the supplied stream.
     *
     * @param in stream to decompress
     */
    public HadoopSnappyCompressorInputStream(final InputStream in) {
        SnappyDecompressor decompressor = new SnappyDecompressor(
                SNAPPY_BUFFER_SIZE_DEFAULT);
        this.decompressorStream = new BlockDecompressorStream(in, decompressor,
                SNAPPY_BUFFER_SIZE_DEFAULT);
    }

    @Override
    public int read() throws IOException {
        final int read = decompressorStream.read();
        count(read);
        return read;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        final int read = decompressorStream.read(b, off, len);
        count(read);
        return read;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        final int read = decompressorStream.read(b);
        count(read);
        return read;
    }

    @Override
    public long skip(final long n) throws IOException {
        final long read = decompressorStream.skip(n);
        count(read);
        return read;
    }

    @Override
    public int available() throws IOException {
        return decompressorStream.available();
    }

    @Override
    public void close() throws IOException {
        decompressorStream.close();
    }

    @Override
    public boolean markSupported() {
        return decompressorStream.markSupported();
    }

    @Override
    public void mark(final int readlimit) {
        decompressorStream.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        decompressorStream.reset();
    }
}
