/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Test
public class MantaCompressionTypeTest {
    public void canDecompressBzip2() throws Exception {
        canDecompress(MantaCompressionType.BZIP2);
    }

    public void canDecompressLz4() throws Exception {
        canDecompress(MantaCompressionType.LZ4);
    }

    public void canDecompressGzip() throws Exception {
        canDecompress(MantaCompressionType.GZIP);
    }

    public void canDecompressSnappy() throws Exception {
        canDecompress(MantaCompressionType.SNAPPY);
    }

    public void canDecompressXz() throws Exception {
        canDecompress(MantaCompressionType.XZ);
    }

    private void canDecompress(MantaCompressionType compressionType)
            throws Exception {
        final String expected = "Hello World";
        byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
        byte[] compressed;

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try (CompressorOutputStream cout = MantaCompressionType.COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(compressionType.getCompressorName(), bout)) {
            cout.write(expectedBytes);
            cout.flush();
        }

        compressed = bout.toByteArray();
        bout.close();

        byte[] actualBytes = new byte[expectedBytes.length];

        try (ByteArrayInputStream bin = new ByteArrayInputStream(compressed);
             InputStream cin = compressionType.createStream(bin)) {
            cin.read(actualBytes);
            int next = cin.read();
            Assert.assertEquals(next, -1,
                    "There should be no more bytes in the stream");
            String actual = new String(actualBytes, StandardCharsets.UTF_8);
            Assert.assertEquals(actual, expected);
        }
    }
}
