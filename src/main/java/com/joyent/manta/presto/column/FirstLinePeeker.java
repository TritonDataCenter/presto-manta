/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.google.common.net.MediaType;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.presto.exceptions.MantaPrestoExceptionUtils;
import com.joyent.manta.presto.exceptions.MantaPrestoFileFormatException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Scanner;

/**
 * Helper class that reads the first non-blank line from a remote Manta file and
 * then aborts the connection on close().
 */
public class FirstLinePeeker implements AutoCloseable {
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private final MantaObjectInputStream inputStream;

    public FirstLinePeeker(final MantaObjectInputStream inputStream) {
        this.inputStream = Objects.requireNonNull(inputStream,
                "InputStream is null");
    }

    public String readFirstLine() {
        String line;
        long count = 1;

        try (Scanner scanner = new Scanner(inputStream, parseCharset().name())) {
            line = scanner.nextLine();

            // Skip blank lines
            while (StringUtils.isBlank(line)) {
                line = scanner.nextLine();
                count++;
            }
        } catch (NoSuchElementException e) {
            String msg = "Data file doesn't contain a single new line marker";
            MantaPrestoFileFormatException me = new MantaPrestoFileFormatException(msg, e);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(inputStream, me);
            me.setContextValue("linesRead", count);

            throw me;
        }

        if (StringUtils.isBlank(line)) {
            String msg = "Data file contains only blank lines";
            MantaPrestoFileFormatException me = new MantaPrestoFileFormatException(msg);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(inputStream, me);
            me.setContextValue("linesRead", count);

            throw me;
        }

        return line;
    }

    private Charset parseCharset() {
        String contentType = inputStream.getHttpHeaders().getContentType();

        if (StringUtils.isBlank(contentType)) {
            return DEFAULT_CHARSET;
        }

        try {
            return MediaType.parse(contentType).charset().or(DEFAULT_CHARSET);
        } catch (IllegalArgumentException e) {
            LoggerFactory.getLogger(getClass())
                    .warn("Illegal character set on content-type: {}", contentType);
            return DEFAULT_CHARSET;
        }
    }

    @Override
    public void close() throws IOException {
        inputStream.abortConnection();
    }
}
