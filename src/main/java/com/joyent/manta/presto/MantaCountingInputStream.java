/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.google.common.io.CountingInputStream;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.presto.compression.MantaCompressionType;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/**
 * A custom {@link InputStream} that wraps a {@link MantaObjectInputStream}
 * in a buffered stream and a counting stream.
 * @since 1.0.0
 */
public class MantaCountingInputStream extends InputStream implements MantaObject {
    private static final int STREAM_BUFFER_SIZE = 65536;

    private final MantaObject mantaObject;
    private final InputStream decompressingStream;
    private final BufferedInputStream bufferedInputStream;
    private final CountingInputStream finalByteCountingStream;

    /**
     * Creates a new {@link InputStream} that wraps a {@link MantaObjectInputStream}
     * in a buffered stream and a counting stream.
     *
     * @param mantaObjectInputStream stream to wrap
     */
    public MantaCountingInputStream(final MantaObjectInputStream mantaObjectInputStream) {
        this.mantaObject = mantaObjectInputStream;
        this.decompressingStream = MantaCompressionType.wrapMantaStreamIfCompressed(mantaObjectInputStream);
        this.bufferedInputStream = new BufferedInputStream(decompressingStream, STREAM_BUFFER_SIZE);
        this.finalByteCountingStream = new CountingInputStream(bufferedInputStream);
    }

    /**
     * Creates a new {@link InputStream} that wraps any {@link InputStream}
     * in a buffered stream and a counting stream.
     *
     * @param anyInputStream stream to wrap
     * @param object Manta object to derive path information from
     */
    public MantaCountingInputStream(final InputStream anyInputStream,
                                    final MantaObject object) {
        this.mantaObject = object;
        this.decompressingStream = MantaCompressionType.wrapMantaStreamIfCompressed(object, anyInputStream);
        this.bufferedInputStream = new BufferedInputStream(decompressingStream, STREAM_BUFFER_SIZE);
        this.finalByteCountingStream = new CountingInputStream(bufferedInputStream);
    }

    public long getCount() {
        return finalByteCountingStream.getCount();
    }

    @Override
    public int read() throws IOException {
        return finalByteCountingStream.read();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        return finalByteCountingStream.read(b, off, len);
    }

    @Override
    public long skip(final long n) throws IOException {
        return finalByteCountingStream.skip(n);
    }

    @Override
    public void mark(final int readlimit) {
        finalByteCountingStream.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        finalByteCountingStream.reset();
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return finalByteCountingStream.read(b);
    }

    @Override
    public int available() throws IOException {
        return finalByteCountingStream.available();
    }

    @Override
    public void close() throws IOException {
        finalByteCountingStream.close();
    }

    @Override
    public boolean markSupported() {
        return finalByteCountingStream.markSupported();
    }

    @Override
    public String getPath() {
        return mantaObject.getPath();
    }

    @Override
    public Long getContentLength() {
        return mantaObject.getContentLength();
    }

    @Override
    public String getContentType() {
        return mantaObject.getContentType();
    }

    @Override
    public String getEtag() {
        return mantaObject.getEtag();
    }

    @Override
    public Date getLastModifiedTime() {
        return mantaObject.getLastModifiedTime();
    }

    @Override
    public String getMtime() {
        return mantaObject.getMtime();
    }

    @Override
    public String getType() {
        return mantaObject.getType();
    }

    @Override
    public MantaHttpHeaders getHttpHeaders() {
        return mantaObject.getHttpHeaders();
    }

    @Override
    public Object getHeader(final String fieldName) {
        return mantaObject.getHeader(fieldName);
    }

    @Override
    public String getHeaderAsString(final String fieldName) {
        return mantaObject.getHeaderAsString(fieldName);
    }

    @Override
    public MantaMetadata getMetadata() {
        return mantaObject.getMetadata();
    }

    @Override
    public byte[] getMd5Bytes() {
        return mantaObject.getMd5Bytes();
    }

    @Override
    public boolean isDirectory() {
        return mantaObject.isDirectory();
    }

    @Override
    public String getRequestId() {
        return mantaObject.getRequestId();
    }
}
