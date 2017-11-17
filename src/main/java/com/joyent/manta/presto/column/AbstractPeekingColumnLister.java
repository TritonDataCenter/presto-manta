/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.presto.compression.MantaCompressionType;
import com.joyent.manta.presto.MantaConnectorId;
import com.joyent.manta.presto.exceptions.MantaPrestoExceptionUtils;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import com.joyent.manta.presto.exceptions.MantaPrestoTableNotFoundException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;

import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

import static com.joyent.manta.presto.tables.MantaLogicalTableProvider.TABLE_DEFINITION_FILENAME;
import static java.util.Objects.requireNonNull;

/**
 * Base implementation of a {@link ColumnLister} that works by looking at the
 * first line of a new line delimited streaming data source.
 *
 * @since 1.0.0
 */
public abstract class AbstractPeekingColumnLister implements ColumnLister  {
    private MantaConnectorId connectorId;
    private MantaClient mantaClient;
    private int maxBytesPerLine;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param connectorId presto connection id object for debugging
     * @param mantaClient object that allows for direct operations on Manta
     * @param maxBytesPerLine number of bytes from the start of file to request
     *                        via a range request so we don't have to download
     *                        the entire file
     */
    public AbstractPeekingColumnLister(final MantaConnectorId connectorId,
                                       final MantaClient mantaClient,
                                       final Integer maxBytesPerLine) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.mantaClient = requireNonNull(mantaClient, "Manta client is null");
        this.maxBytesPerLine = requireNonNull(maxBytesPerLine, "max bytes per line is null");
    }

    protected MantaClient getMantaClient() {
        return mantaClient;
    }

    protected MantaConnectorId getConnectorId() {
        return connectorId;
    }

    /**
     * Finds the smallest file object within the logical table space (matching
     * the table filters).
     *
     * @param tableName table name object with schema specified
     * @param table logical table definition
     * @return the smallest first object found
     *
     * @throws MantaPrestoTableNotFoundException if no objects can be found
     */
    protected MantaObject firstObjectForTable(
            final MantaSchemaTableName tableName, final MantaLogicalTable table) {
        final Optional<MantaObject> first;

        try (Stream<MantaObject> find = mantaClient
                .find(table.getRootPath(), table.directoryFilter())
                .filter(table.filter())
                .filter(obj -> !obj.isDirectory())
                .filter(obj -> !obj.getPath().endsWith(TABLE_DEFINITION_FILENAME))
                .sorted(Comparator.comparingLong(MantaObject::getContentLength))) {
            first = find.findFirst();
        }

        if (!first.isPresent()) {
            String msg = "No objects found for table";
            MantaPrestoTableNotFoundException me = new MantaPrestoTableNotFoundException(
                    tableName, msg);
            me.addContextValue("connectorId", connectorId);
            throw me;
        }

        return first.get();
    }

    /**
     * Reads the first line from the supplied stream.
     *
     * @param objectPath path to file object
     * @return first line of file object
     */
    protected String readFirstLine(final String objectPath) {
        final MantaObjectInputStream in = objectStream(objectPath);

        try {
            FirstLinePeeker peeker = new FirstLinePeeker(in);
            return peeker.readFirstLine();
        } catch (MantaPrestoRuntimeException e) {
            throw e;
            // Wrap uncaught runtime exceptions with additional details
        } catch (RuntimeException e) {
            String msg = "Problem peeking at the first line of remote file";
            MantaPrestoRuntimeException me = new MantaPrestoRuntimeException(msg, e);
            me.setContextValue("connectorId", connectorId);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(in, me);

            throw me;
        } finally {
            Closeables.closeQuietly(in);
        }
    }

    /**
     * Creates and opens an {@link java.io.InputStream} to a remote
     * Manta object.
     *
     * @param objectPath path to file object
     * @return new stream instance containing data of file object
     */
    protected MantaObjectInputStream objectStream(final String objectPath) {
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
            me.setContextValue("connectorId", connectorId);
            me.addContextValue("path", objectPath);

            throw me;
        }
    }
}
