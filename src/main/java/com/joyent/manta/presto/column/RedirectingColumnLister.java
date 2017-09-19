/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.io.Files;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.presto.MantaPrestoFileType;
import com.joyent.manta.presto.MantaPrestoUtils;
import com.joyent.manta.presto.column.json.JsonFileColumnLister;
import com.joyent.manta.presto.exceptions.*;
import com.joyent.manta.util.MantaUtils;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class RedirectingColumnLister {

    /**
     * Map relating configured schema name to Manta directory path.
     */
    private final Map<String, String> schemaMapping;

    /**
     * Manta client instance.
     */
    private final MantaClient mantaClient;

    private final JsonFileColumnLister jsonLister;

    /**
     * Creates a new instance with the required properties.
     *
     * @param schemaMapping map relating configured schema name to Manta directory path
     * @param jsonLister lister instance for processing JSON columns
     * @param mantaClient Manta client instance
     */
    @Inject
    public RedirectingColumnLister(@Named("SchemaMapping") final Map<String, String> schemaMapping,
                                   final JsonFileColumnLister jsonLister,
                                   final MantaClient mantaClient) {
        this.schemaMapping = schemaMapping;
        this.jsonLister = jsonLister;
        this.mantaClient = mantaClient;
    }

    public List<ColumnMetadata> listColumns(final String schemaName, final String tableName) {
        requireNonNull(schemaName, "Schema name is null");
        requireNonNull(tableName, "Table name is null");

        String objectPath = objectPath(schemaName, tableName);

        MantaObjectInputStream in = objectStream(objectPath);
        final String firstLine;
        final String contentType;

        try {
            firstLine = readFirstLine(in);
            contentType = in.getContentType();
        } finally {
            try {
                in.abortConnection();
            } catch (IOException e) {
                LoggerFactory.getLogger(getClass())
                        .info("Error aborting connection after reading first line", e);
            }
        }

        final String extension = Files.getFileExtension(objectPath);
        final String mediaType = MantaPrestoUtils.extractMediaTypeFromContentType(contentType);
        final MantaPrestoFileType type = determineFileType(extension, mediaType, in);
        final ColumnLister lister;

        switch (type) {
            case LDJSON:
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
     * Determines what {@link MantaPrestoFileType} is associated with
     * an object via looking up metadata on its file extension or
     * media type.
     *
     * @param extension file extension to query for an association
     * @param mediaType media type to query for an association
     * @param object object reference for debugging information
     * @return instance of enum associated with object's file type
     */
    private MantaPrestoFileType determineFileType(final String extension,
                                                  final String mediaType,
                                                  final MantaObject object) {
        if (MantaPrestoFileType.isSupportedFileTypeByExtension(extension)) {
            return MantaPrestoFileType.valueByExtension(extension);
        } else if (MantaPrestoFileType.isSupportFileTypeByMediaType(mediaType)) {
            return MantaPrestoFileType.valueByMediaType(mediaType);
        } else {
            String msg = "Table name (Manta object) specified is not a "
                    + "supported file type";
            MantaPrestoIllegalArgumentException me = new MantaPrestoIllegalArgumentException(msg);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(object, me);
            throw me;
        }
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
        MantaObjectInputStream in = null;

        try {
            return mantaClient.getAsInputStream(objectPath);
        } catch (IOException e) {
            String msg = "Problem opening InputStream to Manta object";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.addContextValue("path", objectPath);

            throw me;
        }
    }

    /**
     * Resolves the object path of a remote Manta object based on the passed
     * schema name and table name.
     *
     * @param schemaName Presto schema name
     * @param tableName Table name
     * @return path to file object in Manta
     */
    private String objectPath(final String schemaName, final String tableName) {
        String directory = schemaMapping.get(schemaName);

        if (directory == null) {
            throw MantaPrestoSchemaNotFoundException.withNoDirectoryMessage(
                    schemaName);
        }
        return MantaUtils.formatPath(directory
                + MantaClient.SEPARATOR + tableName);
    }
}
