/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.compression;

import org.apache.commons.compress.compressors.CompressorInputStream;
import org.xerial.snappy.SnappyInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Apache Compress compatible {@link CompressorInputStream} that wraps the
 * Xerial {@link SnappyInputStream} so that the Xerial libraries will talk
 * with the Apache libraries.
 *
 * @since 1.0.0
 */
public class XerialSnappyCompressorInputStream extends CompressorInputStream {
    private final SnappyInputStream snappyInputStream;

    /**
     * Creates a new instance that decompresses the supplied stream.
     *
     * @param in stream to decompress
     * @throws IOException thrown when there is a problem decompressing initially
     */
    public XerialSnappyCompressorInputStream(final InputStream in) throws IOException {
        this.snappyInputStream = new SnappyInputStream(in);
    }

    @Override
    public void close() throws IOException {
        snappyInputStream.close();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        final int read = snappyInputStream.read(b, off, len);
        count(read);
        return read;
    }

    @Override
    public int read() throws IOException {
        final int read = snappyInputStream.read();
        count(read);
        return read;
    }

    @Override
    public int available() throws IOException {
        final int read = snappyInputStream.available();
        count(read);
        return read;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        final int read = snappyInputStream.read(b);
        count(read);
        return read;
    }

    @Override
    public long skip(final long n) throws IOException {
        final long read = snappyInputStream.skip(n);
        count(read);
        return read;
    }

    @Override
    public void mark(final int readlimit) {
        snappyInputStream.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        snappyInputStream.reset();
    }

    @Override
    public boolean markSupported() {
        return snappyInputStream.markSupported();
    }
}
