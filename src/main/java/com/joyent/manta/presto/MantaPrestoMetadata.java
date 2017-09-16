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
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.org.apache.commons.io.FilenameUtils;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
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

    /**
     * List of directories that are typically available at the account root
     * of every Manta user.
     */
    static final List<String> DEFAULT_SCHEMAS = ImmutableList.of(
            "jobs", "public", "reports", "stor");

    private final String connectorId;
    private final MantaClient mantaClient;
    private final MantaPrestoClient MantaPrestoClient;

    @Inject
    public MantaPrestoMetadata(final MantaPrestoConnectorId connectorId,
                               final MantaPrestoClient MantaPrestoClient,
                               final MantaClient mantaClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.MantaPrestoClient = requireNonNull(MantaPrestoClient, "client is null");
        this.mantaClient = requireNonNull(mantaClient, "Manta client instance is null");
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
        final ConfigContext config = mantaClient.getContext();
        final String home = config.getMantaHomeDirectory();

        try {
            return mantaClient
                    .listObjects(home)
                    .filter(MantaObject::isDirectory)
                    .map(obj -> FilenameUtils.getName(obj.getPath()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            if (e instanceof MantaClientHttpResponseException) {
                MantaClientHttpResponseException mchre = (MantaClientHttpResponseException)e;
                log.debug("Returning default list of schemas because actual list "
                        + "isn't accessible for user");

                /* If Manta is being accessed as a subuser, there will be no access
                 * available to the root directory, so we just return the default
                 * root directories available to a user as schemas. */
                if (mchre.getServerCode().equals(MantaErrorCode.NO_MATCHING_ROLE_TAG_ERROR)) {
                    return DEFAULT_SCHEMAS;
                }
            }

            String msg = "Unable to list schema names (root Manta directory)";
            MantaPrestoRuntimeException re = new MantaPrestoRuntimeException(msg, e);
            re.setContextValue("homeDirectory", home);
            throw re;
        }
    }

    @Override
    public List<SchemaTableName> listTables(final ConnectorSession session,
                                            final String schemaNameAsPath) {
        Validate.notBlank(schemaNameAsPath, "Schema name is blank");

        final String directory = MantaUtils.formatPath(schemaNameAsPath);

        try {
            return mantaClient
                    .listObjects(directory)
                    .filter(obj -> !obj.isDirectory())
                    .map(MantaPrestoSchemaTableName::new)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            if (e instanceof MantaClientHttpResponseException) {
                MantaClientHttpResponseException mchre = (MantaClientHttpResponseException)e;

                if (mchre.getServerCode().equals(MantaErrorCode.RESOURCE_NOT_FOUND_ERROR)) {
                    String msg = "The schema (Manta directory) specified couldn't be found";
                    MantaPrestoSchemaNotFoundException me = new MantaPrestoSchemaNotFoundException(msg);
                    me.setContextValue("schemaPath", directory);
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
    public MantaPrestoTableHandle getTableHandle(final ConnectorSession session,
                                                 final SchemaTableName tableName) {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        MantaPrestoTable table = MantaPrestoClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new MantaPrestoTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            final ConnectorSession session, final ConnectorTableHandle table,
            final Constraint<ColumnHandle> constraint,
            final Optional<Set<ColumnHandle>> desiredColumns) {
        MantaPrestoTableHandle tableHandle = (MantaPrestoTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new MantaPrestoTableLayoutHandle(tableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(final ConnectorSession session,
                                               final ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(final ConnectorSession session,
                                                   final ConnectorTableHandle table) {
        MantaPrestoTableHandle MantaPrestoTableHandle = (MantaPrestoTableHandle) table;

        if (!MantaPrestoTableHandle.getConnectorId().equals(connectorId)) {
            throw new IllegalArgumentException("tableHandle is not for this connector");
        }

        SchemaTableName tableName = new SchemaTableName(MantaPrestoTableHandle.getSchemaName(), MantaPrestoTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(final ConnectorSession session,
                                                      final ConnectorTableHandle tableHandle) {
        MantaPrestoTableHandle MantaPrestoTableHandle = (MantaPrestoTableHandle) tableHandle;

        if (!MantaPrestoTableHandle.getConnectorId().equals(connectorId)) {
            throw new IllegalArgumentException("tableHandle is not for this connector");
        }

        MantaPrestoTable table = MantaPrestoClient.getTable(MantaPrestoTableHandle.getSchemaName(), MantaPrestoTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(MantaPrestoTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new MantaPrestoColumnHandle(connectorId, column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            final ConnectorSession session, final SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(final SchemaTableName tableName) {
        MantaPrestoTable table = MantaPrestoClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(final ConnectorSession session, final SchemaTablePrefix prefix) {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
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
}
