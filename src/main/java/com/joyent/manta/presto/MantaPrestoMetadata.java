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
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaPrestoMetadata implements ConnectorMetadata {
    private final String connectorId;

    private final MantaPrestoClient MantaPrestoClient;

    @Inject
    public MantaPrestoMetadata(final MantaPrestoConnectorId connectorId,
                               final MantaPrestoClient MantaPrestoClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.MantaPrestoClient = requireNonNull(MantaPrestoClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(final ConnectorSession session) {
        return listSchemaNames();
    }

    public List<String> listSchemaNames() {
        return ImmutableList.copyOf(MantaPrestoClient.getSchemaNames());
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
    public List<SchemaTableName> listTables(final ConnectorSession session,
                                            final String schemaNameOrNull) {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        } else {
            schemaNames = MantaPrestoClient.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : MantaPrestoClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
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
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

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
                .append("MantaPrestoClient", MantaPrestoClient)
                .toString();
    }
}
