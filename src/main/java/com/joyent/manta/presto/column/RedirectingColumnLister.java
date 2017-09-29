/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.google.common.io.Files;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.presto.MantaCompressionType;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.exceptions.MantaPrestoExceptionUtils;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import com.joyent.manta.presto.record.json.MantaJsonFileColumnLister;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class RedirectingColumnLister {
    /**
     * Manta client instance.
     */
    private final MantaClient mantaClient;

    private final int maxBytesPerLine;

    private final MantaJsonFileColumnLister jsonLister;

    /**
     * Creates a new instance with the required properties.
     *
     * @param maxBytesPerLine maximum number of bytes to read per line
     * @param jsonLister lister instance for processing JSON columns
     * @param mantaClient Manta client instance
     */
    @Inject
    public RedirectingColumnLister(@Named("MaxBytesPerLine") final Integer maxBytesPerLine,
                                   final MantaJsonFileColumnLister jsonLister,
                                   final MantaClient mantaClient) {
        this.maxBytesPerLine = requireNonNull(maxBytesPerLine, "Max bytes per line is null");
        this.jsonLister = requireNonNull(jsonLister, "Json lister is null");
        this.mantaClient = requireNonNull(mantaClient, "Manta client is null");
    }

    public List<MantaColumn> listColumns(final String objectPath, final MantaDataFileType type) {
        final String firstLine;

        try (MantaObjectInputStream in = objectStream(objectPath)) {
            firstLine = readFirstLine(in);
        } catch (IOException e) {
            String msg = "Error reading first line of file object";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.setContextValue("objectPath", objectPath);
            throw me;
        }

        final ColumnLister lister;

        switch (type) {
            case NDJSON:
                lister = jsonLister;
                break;
            case CSV:
            default:
                String msg = "Unknown file type enum resolved";
                MantaPrestoIllegalArgumentException me = new MantaPrestoIllegalArgumentException(msg);
                me.addContextValue("type", type);
                throw me;
        }

        return lister.listColumns(objectPath, type, firstLine);
    }

    /**
     * Reads the first line from the supplied stream.
     *
     * @param in stream of file object
     * @return first line of file object
     */
    private String readFirstLine(final MantaObjectInputStream in) {
        try {
            FirstLinePeeker peeker = new FirstLinePeeker(in);
            return peeker.readFirstLine();
        } catch (MantaPrestoRuntimeException e) {
            throw e;
        // Wrap uncaught runtime exceptions with additional details
        } catch (RuntimeException e) {
            String msg = "Problem peeking at the first line of remote file";
            MantaPrestoRuntimeException me = new MantaPrestoRuntimeException(msg, e);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(in, me);

            throw me;
        }
    }

    /**
     * Creates and opens an {@link java.io.InputStream} to a remote
     * Manta object.
     *
     * @param objectPath path to file object
     * @return new stream instance containing data of file object
     */
    private MantaObjectInputStream objectStream(final String objectPath) {
        try {
            MantaHttpHeaders headers = new MantaHttpHeaders();

            String extension = Files.getFileExtension(objectPath);

            // Don't do HTTP range requests on compressed files
            if (!MantaCompressionType.isExtensionSupported(extension)) {
                headers.setRange(String.format("bytes=0-%d", maxBytesPerLine));
            }

            return mantaClient.getAsInputStream(objectPath, headers);
        } catch (IOException e) {
            String msg = "Problem opening InputStream to Manta object";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.addContextValue("path", objectPath);

            throw me;
        }
    }
}
