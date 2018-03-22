package com.joyent.manta.presto;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Test
public class MantaPrestoUtilsTest {
    public void canExtractMediaTypeFromContentType() {
        final String contentType = "text/plain; charset=\"UTF-8\"";
        final String mediaType = MantaPrestoUtils.extractMediaTypeFromContentType(contentType);
        Assert.assertEquals("text/plain", mediaType,
                "Unable to extract media type");
    }

    public void canExtractCharsetFromContentType() {
        final String contentType = "text/plain; charset=\"UTF-8\"";
        final Charset charset = MantaPrestoUtils.parseCharset(contentType,
                StandardCharsets.US_ASCII);
        Assert.assertEquals(StandardCharsets.UTF_8, charset);
    }

    public void canSubstituteHomeDirectoryInPath() {
        final String homeDir = "/user";
        final String path = "~~/stor/foo/bar";
        final String expected = "/user/stor/foo/bar";
        final String actual = MantaPrestoUtils.substituteHomeDirectory(path, homeDir);
        Assert.assertEquals(expected, actual);
    }

    public void verifySubstitutionOfHomeDirectoryInPathWontCorrupt() {
        final String homeDir = "/user";
        final String path = "/user/stor/foo/baz";
        final String expected = "/user/stor/foo/baz";
        final String actual = MantaPrestoUtils.substituteHomeDirectory(path, homeDir);
        Assert.assertEquals(expected, actual);
    }
}
