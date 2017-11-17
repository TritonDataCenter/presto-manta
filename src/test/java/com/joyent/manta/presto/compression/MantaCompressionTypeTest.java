/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.compression;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static com.joyent.manta.presto.compression.MantaCompressionType.BZIP2;
import static com.joyent.manta.presto.compression.MantaCompressionType.COMPRESSOR_STREAM_FACTORY;
import static com.joyent.manta.presto.compression.MantaCompressionType.GZIP;
import static com.joyent.manta.presto.compression.MantaCompressionType.HADOOP_SNAPPY;
import static com.joyent.manta.presto.compression.MantaCompressionType.LZ4;
import static com.joyent.manta.presto.compression.MantaCompressionType.XERIAL_SNAPPY;
import static com.joyent.manta.presto.compression.MantaCompressionType.XZ;

@Test
public class MantaCompressionTypeTest {
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    public void canDecompressBzip2() throws Exception {
        canDecompress(BZIP2, compressedTestString(BZIP2));
    }

    public void canDecompressLz4() throws Exception {
        canDecompress(LZ4, compressedTestString(LZ4));
    }

    public void canDecompressGzip() throws Exception {
        canDecompress(GZIP, compressedTestString(GZIP));
    }

    public void canDecompressXerialSnappy() throws Exception {
        final String resourcePath = "test-data/compressed/hello-world.txt.xsnappy";
        final byte[] expected;
        try (InputStream in = classLoader.getResourceAsStream(resourcePath)) {
            expected = ByteStreams.toByteArray(in);
        }

        canDecompress(XERIAL_SNAPPY, expected);
    }

    public void canDecompressHadoopSnappy() throws Exception {
        final String resourcePath = "test-data/compressed/hello-world.txt.snappy";
        final byte[] expected;
        try (InputStream in = classLoader.getResourceAsStream(resourcePath)) {
            expected = ByteStreams.toByteArray(in);
        }

        try {
            canDecompress(HADOOP_SNAPPY, expected);
        } catch (UnsupportedOperationException e) {
            throw new SkipException("Native libraries need to be loaded in "
                    + "order to use decompression algorithm");
        }
    }

    public void canDecompressXz() throws Exception {
        canDecompress(XZ, compressedTestString(XZ));
    }

    private byte[] compressedTestString(MantaCompressionType compressionType) throws Exception {
        final String expected = "Hello World";
        byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try (CompressorOutputStream cout = COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(compressionType.getCompressorName(), bout)) {
            cout.write(expectedBytes);
            cout.flush();
        }
        bout.close();

        return bout.toByteArray();
    }

    private void canDecompress(MantaCompressionType compressionType, byte[] compressed)
            throws Exception {
        final String expected = "Hello World";

        try (ByteArrayInputStream bin = new ByteArrayInputStream(compressed);
             InputStream cin = compressionType.createStream(bin)) {

            String actual = CharStreams.toString(new InputStreamReader(
                    cin, StandardCharsets.UTF_8));

            Assert.assertEquals(actual, expected);
        }
    }
}
