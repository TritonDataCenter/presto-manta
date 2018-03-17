/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.VarcharType;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.column.MantaPartitionColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import com.joyent.manta.presto.exceptions.MantaPrestoUnexpectedClass;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaLogicalTablePartitionDefinition;
import com.joyent.manta.presto.tables.MantaSchemaTableName;
import io.airlift.slice.Slice;

import javax.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.joyent.manta.presto.tables.MantaLogicalTableProvider.TABLE_DEFINITION_FILENAME;
import static java.util.Objects.requireNonNull;

/**
 * Class that creates a new {@link ConnectorSplitSource} per file object in
 * the Manta logical table in which we are operating.
 *
 * @since 1.0.0
 */
public class MantaSplitManager implements ConnectorSplitManager {
    private final String connectorId;
    private final MantaClient mantaClient;

    /**
     * Creates a new instance.
     *
     * @param connectorId connector id used for debugging
     * @param mantaClient manta client allowing for direct operation on Manta
     */
    @Inject
    public MantaSplitManager(final MantaConnectorId connectorId,
                             final MantaClient mantaClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.mantaClient = requireNonNull(mantaClient, "Manta client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(final ConnectorTransactionHandle handle,
                                          final ConnectorSession session,
                                          final ConnectorTableLayoutHandle layout) {
        if (!layout.getClass().equals(MantaTableLayoutHandle.class)) {
            throw new MantaPrestoUnexpectedClass(MantaTableLayoutHandle.class,
                    handle.getClass());
        }

        final MantaTableLayoutHandle layoutHandle = (MantaTableLayoutHandle)layout;
        final TupleDomain<ColumnHandle> predicate = layoutHandle.getPredicate();
        final MantaSchemaTableName tableName = layoutHandle.getTableName();
        final MantaLogicalTable table = tableName.getTable();

        final MantaSplitPartitionPredicate filePartitionPredicate;
        final MantaSplitPartitionPredicate dirPartitionPredicate;

        if (predicate != null && predicate.getDomains().isPresent()
                && table.getPartitionDefinition().isPresent()) {
            final Map<ColumnHandle, Domain> domains = predicate.getDomains().get();
            final MantaLogicalTablePartitionDefinition partitionDefinition = table.getPartitionDefinition().get();

            filePartitionPredicate = createPartitionPredicate(
                    partitionDefinition.getFilterRegex(),
                    domains,
                    partitionDefinition.filePartitionsAsColumns());

            dirPartitionPredicate = createPartitionPredicate(
                    partitionDefinition.getDirectoryFilterRegex(),
                    domains,
                    partitionDefinition.directoryPartitionsAsColumns());
        } else {
            filePartitionPredicate = MantaSplitPartitionPredicate.ALWAYS_TRUE;
            dirPartitionPredicate = MantaSplitPartitionPredicate.ALWAYS_TRUE;
        }

        /* Combines partitioning and filter such that we can limit the directories
         * in the search space. */
        final Predicate<MantaObject> directoryPredicate =
                dirPartitionPredicate.and(table.directoryFilter());

        Stream<MantaObject> objectStream = mantaClient.find(table.getRootPath(), directoryPredicate)
                .filter(table.filter())
                .filter(obj -> !obj.isDirectory())
                .filter(obj -> !obj.getPath().endsWith(TABLE_DEFINITION_FILENAME))
                .filter(filePartitionPredicate);

        return new MantaStreamingSplitSource(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getDataFileType(),
                objectStream,
                filePartitionPredicate,
                dirPartitionPredicate);
    }

    /**
     * Creates a new predicate that tests a {@link MantaObject} to see if its
     * filename matches the regular expression groups that were specified in the
     * partition definition and that the contents of the group matches equal
     * the values specified in the query's WHERE clause.
     *
     * @param partitionRegex regular expression to use for filename filtering
     * @param domains Presto object containing WHERE clause values
     * @param partitionColumns columns in which we have enabled partitioning
     *
     * @return a new predicate object that allows for filtering only matching files
     */
    static MantaSplitPartitionPredicate createPartitionPredicate(final Pattern partitionRegex,
                                                           final Map<ColumnHandle, Domain> domains,
                                                           final List<MantaPartitionColumn> partitionColumns) {
        if (partitionRegex == null || partitionColumns.isEmpty() || domains.isEmpty()) {
            return MantaSplitPartitionPredicate.ALWAYS_TRUE;
        }

        final LinkedHashMap<MantaPartitionColumn, String> partitionColumnToMatchValue =
                partitionColumnToMatchValue(domains, partitionColumns);

        return new MantaSplitPartitionPredicate(partitionColumnToMatchValue, partitionRegex);
    }

    /**
     * Creates a insertion ordered {@link Map} where we can relate the partition
     * column to the actual partition value specified by a user in the
     * WHERE clause of a query.
     *
     * @param domains Presto object containing WHERE clause values
     * @param partitionColumns columns in which we have enabled partitioning
     *
     * @return map with (1+) index to literal partition match value
     */
    static LinkedHashMap<MantaPartitionColumn, String> partitionColumnToMatchValue(
            final Map<ColumnHandle, Domain> domains,
            final List<MantaPartitionColumn> partitionColumns) {

        final LinkedHashMap<MantaPartitionColumn, String> builder =
                new LinkedHashMap<>(domains.size());

        for (MantaPartitionColumn c : partitionColumns) {
            final Domain domain = domains.get(c);

            if (domain != null) {
                validateDomainForPartition(domain);

                @SuppressWarnings("unchecked")
                final Slice slice = (Slice)domain.getSingleValue();
                final String value = slice.toStringUtf8();

                builder.put(c, value);
            }
        }

        return builder;
    }

    private static void validateDomainForPartition(final Domain domain) {
        if (!domain.isSingleValue()) {
            String msg = "Expected query parameter for partition to be a single value";
            MantaPrestoIllegalArgumentException e = new MantaPrestoIllegalArgumentException(msg);
            e.setContextValue("domain", domain);
            throw e;
        }

        if (!domain.getType().equals(VarcharType.VARCHAR)) {
            String msg = "Expected query parameter for partition to be a varchar";
            MantaPrestoIllegalArgumentException e = new MantaPrestoIllegalArgumentException(msg);
            e.setContextValue("domain", domain);
            e.setContextValue("expectedType", VarcharType.VARCHAR);
            e.setContextValue("actualType", domain.getType());
            throw e;
        }
    }
}
