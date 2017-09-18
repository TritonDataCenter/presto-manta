/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.presto.column.FirstLinePeeker;
import com.joyent.manta.presto.column.MantaPrestoColumnHandle;
import com.joyent.manta.presto.exceptions.MantaPrestoExceptionUtils;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import com.joyent.manta.presto.exceptions.MantaPrestoSchemaNotFoundException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import com.joyent.manta.presto.exceptions.MantaPrestoUnexpectedClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * <p>Class that provides methods for listing schemas, tables, and columns. This
 * class contains the domain mapping logic between Presto and Manta.</p>
 *
 * <p>Presto to Manta concept mapping:</p>
 * <table>
 *  <tr><th>Presto</th><th>Manta</th></tr>
 *  <tr><td>Schema</td><td>Directory</td>
 *  <tr><td>Table</td><td>File</td></tr>
 *  <tr><td>Column</td><td>Line within file</td></tr>
 * </table>
 */
public class MantaPrestoMetadata implements ConnectorMetadata {
    /**
     * Logger instance.
     */
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String connectorId;
    private final MantaClient mantaClient;
    private final MantaPrestoClient MantaPrestoClient;
    private final Map<String, String> schemaMapping;
    private final ObjectToTableDirectoryListingFilter tableListingFilter;

    @Inject
    public MantaPrestoMetadata(final MantaPrestoConnectorId connectorId,
                               final MantaPrestoClient MantaPrestoClient,
                               @Named("SchemaMapping") final Map<String, String> schemaMapping,
                               final MantaClient mantaClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.MantaPrestoClient = requireNonNull(MantaPrestoClient, "client is null");
        this.mantaClient = requireNonNull(mantaClient, "Manta client instance is null");
        this.tableListingFilter = new ObjectToTableDirectoryListingFilter(mantaClient);
        this.schemaMapping = schemaMapping;
    }

    /**
     * {@inheritDoc}
     *
     * <p>In order to work within Manta's directory mode, this method maps the
     * Manta user's root directory as a list of "schemas".</p>
     *
     * @param session connection session
     * @return list of schemas (root directory list)
     */
    @Override
    public List<String> listSchemaNames(final ConnectorSession session) {
        return ImmutableList.copyOf(schemaMapping.keySet());
    }

    @Override
    public List<SchemaTableName> listTables(final ConnectorSession session,
                                            final String schemaName) {
        final String directory = schemaMapping.get(schemaName);

        if (directory == null) {
            throw unknownSchemaException(schemaName);
        }

        try {
            return mantaClient
                    .listObjects(directory)
                    .filter(tableListingFilter)
                    .map(obj -> {
                        final String relativePath =
                                StringUtils.removeStart(obj.getPath(), directory);

                        return new MantaPrestoSchemaTableName(
                                schemaName, relativePath, directory, relativePath);
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            if (e instanceof MantaClientHttpResponseException) {
                MantaClientHttpResponseException mchre = (MantaClientHttpResponseException)e;

                if (mchre.getServerCode().equals(MantaErrorCode.RESOURCE_NOT_FOUND_ERROR)) {
                    String msg = "Manta directory path resolved from schema "
                            + "does not exist.";
                    MantaPrestoSchemaNotFoundException me =
                            new MantaPrestoSchemaNotFoundException(schemaName, msg);
                    me.setContextValue("schemaNameInBrackets",
                            String.format("[%s]", schemaName));
                    me.setContextValue("remoteDirectory", directory);
                    throw me;
                }
            }

            String msg = "Unable to list tables (Manta directory)";
            MantaPrestoRuntimeException re = new MantaPrestoRuntimeException(msg, e);
            re.setContextValue("path", directory);
            throw re;
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(final ConnectorSession session,
                                               final SchemaTableName tableName) {
        final MantaPrestoSchemaTableName mantaTableName;

        if (tableName.getClass().equals(MantaPrestoSchemaTableName.class)) {
            mantaTableName = (MantaPrestoSchemaTableName)tableName;
        } else {
            String directory = schemaMapping.get(tableName.getSchemaName());

            if (directory == null) {
                throw unknownSchemaException(tableName.getSchemaName());
            }

            // TODO: This probably won't work - let's find out if it is needed
            mantaTableName = new MantaPrestoSchemaTableName(tableName.getSchemaName(),
                    tableName.getTableName(), directory, tableName.getTableName());
        }

        if (!mantaClient.existsAndIsAccessible(mantaTableName.getObjectPath())) {
            return null;
        }

        return mantaTableName;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            final ConnectorSession session,
            final ConnectorTableHandle table,
            final Constraint<ColumnHandle> constraint,
            final Optional<Set<ColumnHandle>> desiredColumns) {
        if (!table.getClass().equals(MantaPrestoTable.class)) {
            throw new MantaPrestoUnexpectedClass(MantaPrestoTable.class,
                    table.getClass());
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(new MantaPrestoTableLayoutHandle((MantaPrestoTable) table));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(final ConnectorSession session,
                                               final ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(final ConnectorSession session,
                                                   final ConnectorTableHandle tableHandle) {
        if (!tableHandle.getClass().equals(MantaPrestoTable.class)) {
            throw new MantaPrestoUnexpectedClass(MantaPrestoTable.class,
                    tableHandle.getClass());
        }

        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(final ConnectorSession session,
                                                      final ConnectorTableHandle tableHandle) {
        if (!tableHandle.getClass().equals(MantaPrestoTable.class)) {
            throw new MantaPrestoUnexpectedClass(MantaPrestoTable.class,
                    tableHandle.getClass());
        }


//        if (!mantaClient.existsAndIsAccessible(objectPath)) {
//            SchemaTableName tableName = new MantaPrestoSchemaTableName(objectPath);
//            String msg = "Table name (Manta directory and file path) didn't match an existing table";
//            MantaPrestoTableNotFoundException me = new MantaPrestoTableNotFoundException(tableName, msg);
//            me.addContextValue("objectPath", objectPath);
//
//            throw me;
//        }

//        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
//        int index = 0;
//        for (ColumnMetadata column : table.getColumnsMetadata()) {
//            columnHandles.put(column.getName(), new MantaPrestoColumnHandle(connectorId, column.getName(), column.getType(), index));
//            index++;
//        }
//        return columnHandles.build();

        // TODO: Write me
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            final ConnectorSession session,
            final SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");

        String path = prefix.getSchemaName() + MantaClient.SEPARATOR + prefix.getTableName();
        String firstLine;

        MantaObjectInputStream in = null;

        try {
            in = mantaClient.getAsInputStream(path);
        } catch (IOException e) {
            String msg = "Problem peeking at the first line of remote file";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(in, me);

            throw me;
        }

        try (FirstLinePeeker peeker = new FirstLinePeeker(in)) {
            firstLine = peeker.readFirstLine();
        } catch (IOException e) {
            String msg = "Problem peeking at the first line of remote file";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(in, me);

            throw me;
        }

        System.out.println(firstLine);

        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(final ConnectorSession session,
                                            final ConnectorTableHandle tableHandle,
                                            final ColumnHandle columnHandle) {
        return ((MantaPrestoColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("connectorId", connectorId)
                .append("mantaClient", mantaClient)
                .toString();
    }

    private MantaPrestoSchemaNotFoundException unknownSchemaException(final String schemaName) {
        String msg = "No mapping to a Manta directory was found for the "
                + "specified schema. Make sure that you have specified "
                + "the schema in your catalog configuration in the "
                + "'manta.schema.<schema name> = <manta directory name>' "
                + "format.";
        MantaPrestoSchemaNotFoundException me =
                new MantaPrestoSchemaNotFoundException(schemaName, msg);
        me.setContextValue("schemaNameInBrackets",
                String.format("[%s]", schemaName));
        return me;
    }
}
