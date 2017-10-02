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
import com.google.common.collect.ImmutableMap;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.column.RedirectingColumnLister;
import com.joyent.manta.presto.exceptions.MantaPrestoSchemaNotFoundException;
import com.joyent.manta.presto.exceptions.MantaPrestoUnexpectedClass;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaLogicalTableProvider;
import com.joyent.manta.presto.tables.MantaSchemaTableName;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * <p>Class that provides methods for listing schemas, tables, and columns. This
 * class contains the domain mapping logic between Presto and Manta.</p>
 */
public class MantaMetadata implements ConnectorMetadata {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String connectorId;
    private final MantaClient mantaClient;
    private final RedirectingColumnLister columnLister;

    /**
     * Map relating configured schema name to Manta directory path.
     */
    private final Map<String, String> schemaMapping;

    /**
     * Provider object that allows you to look up tables by properties.
     */
    private final MantaLogicalTableProvider tableProvider;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param connectorId presto connection id object for debugging
     * @param columnLister listing instance that will redirect to the correct
     *                     lister for the data file type
     * @param schemaMapping schema to directory path mapping
     * @param tableProvider provider object that allows you to look up tables
     *                      by properties
     * @param mantaClient object that allows for direct operations on Manta
     */
    @Inject
    public MantaMetadata(final MantaConnectorId connectorId,
                         final RedirectingColumnLister columnLister,
                         @Named("SchemaMapping") final Map<String, String> schemaMapping,
                         final MantaLogicalTableProvider tableProvider,
                         final MantaClient mantaClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.columnLister = requireNonNull(columnLister, "column lister is null");
        this.mantaClient = requireNonNull(mantaClient, "Manta client instance is null");
        this.tableProvider = requireNonNull(tableProvider, "table provider is null");
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
        return tableProvider.tableListForSchema(schemaName);
    }

    @Override
    public boolean schemaExists(final ConnectorSession session, final String schemaName) {
        return schemaMapping.containsKey(schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(final ConnectorSession session,
                                               final SchemaTableName tableName) {
        final MantaLogicalTable table = tableProvider
                .getTable(tableName.getSchemaName(), tableName.getTableName());

        return new MantaSchemaTableName(tableName.getSchemaName(), table);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            final ConnectorSession session,
            final ConnectorTableHandle table,
            final Constraint<ColumnHandle> constraint,
            final Optional<Set<ColumnHandle>> desiredColumns) {
        if (!table.getClass().equals(MantaSchemaTableName.class)) {
            throw new MantaPrestoUnexpectedClass(MantaSchemaTableName.class,
                    table.getClass());
        }

        MantaSchemaTableName tableName = (MantaSchemaTableName)table;

        ConnectorTableLayout layout = new ConnectorTableLayout(new MantaTableLayoutHandle(tableName));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(final ConnectorSession session,
                                               final ConnectorTableLayoutHandle handle) {
        if (!handle.getClass().equals(MantaTableLayoutHandle.class)) {
            throw new MantaPrestoUnexpectedClass(MantaSchemaTableName.class,
                    handle.getClass());
        }

        MantaTableLayoutHandle tableHandle = (MantaTableLayoutHandle)handle;
        ConnectorTableLayout layout = new ConnectorTableLayout(tableHandle);

        return layout;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(final ConnectorSession session,
                                                   final ConnectorTableHandle tableHandle) {
        if (!tableHandle.getClass().equals(MantaSchemaTableName.class)) {
            throw new MantaPrestoUnexpectedClass(MantaSchemaTableName.class,
                    tableHandle.getClass());
        }

        MantaSchemaTableName tableName = (MantaSchemaTableName)tableHandle;
        final MantaLogicalTable table = tableName.getTable();
        final List<? extends ColumnMetadata> columns = columnLister.listColumns(
                tableName, table);

        @SuppressWarnings("unchecked")
        List<ColumnMetadata> columnMetadata = (List<ColumnMetadata>)columns;

        return new ConnectorTableMetadata(tableName, columnMetadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(final ConnectorSession session,
                                                      final ConnectorTableHandle handle) {
        if (!handle.getClass().equals(MantaSchemaTableName.class)) {
            throw new MantaPrestoUnexpectedClass(MantaSchemaTableName.class,
                    handle.getClass());
        }

        final MantaSchemaTableName tableName = (MantaSchemaTableName)handle;
        final MantaLogicalTable table = tableName.getTable();
        final List<MantaColumn> columns = columnLister.listColumns(tableName, table);

        ImmutableMap.Builder<String, ColumnHandle> builder = new ImmutableMap.Builder<>();

        for (MantaColumn column : columns) {
            builder.put(column.getName(), column);
        }

        return builder.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            final ConnectorSession session,
            final SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        String schemaName = prefix.getSchemaName();

        if (!schemaMapping.containsKey(schemaName)) {
            throw MantaPrestoSchemaNotFoundException.withNoDirectoryMessage(schemaName);
        }

        MantaLogicalTable table = tableProvider.getTable(schemaName, prefix.getTableName());
        MantaSchemaTableName tableName = new MantaSchemaTableName(schemaName, table);
        final List<? extends ColumnMetadata> columns = columnLister.listColumns(
                tableName, table);

        @SuppressWarnings("unchecked")
        List<ColumnMetadata> columnMetadata = (List<ColumnMetadata>)columns;

        return ImmutableMap.of(tableName, columnMetadata);
    }

    @Override
    public ColumnMetadata getColumnMetadata(final ConnectorSession session,
                                            final ConnectorTableHandle tableHandle,
                                            final ColumnHandle columnHandle) {
        return ((MantaColumn)columnHandle);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("connectorId", connectorId)
                .append("mantaClient", mantaClient)
                .toString();
    }
}
