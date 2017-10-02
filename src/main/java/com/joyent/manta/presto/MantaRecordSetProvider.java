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
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.presto.column.MantaColumn;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Class that provides {@link RecordSet} instances based on the columns
 * associated with a table.
 *
 * @since 1.0.0
 */
public class MantaRecordSetProvider implements ConnectorRecordSetProvider {
    private final String connectorId;
    private final MantaClient mantaClient;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param connectorId presto connection id object for debugging
     * @param mantaClient object that allows for direct operations on Manta
     */
    @Inject
    public MantaRecordSetProvider(final MantaConnectorId connectorId,
                                  final MantaClient mantaClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.mantaClient = requireNonNull(mantaClient, "Manta client is null");
    }

    @Override
    public RecordSet getRecordSet(final ConnectorTransactionHandle transactionHandle,
                                  final ConnectorSession session,
                                  final ConnectorSplit split,
                                  final List<? extends ColumnHandle> columns) {
        requireNonNull(split, "partitionChunk is null");
        MantaSplit mantaSplit = (MantaSplit) split;

        if (!mantaSplit.getConnectorId().equals(connectorId)) {
            throw new IllegalArgumentException("split is not for this connector");
        }

        ImmutableList.Builder<MantaColumn> handles = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            handles.add((MantaColumn) column);
        }

        return new MantaRecordSet(mantaSplit, handles.build(), mantaClient);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("connectorId", connectorId)
                .toString();
    }
}
