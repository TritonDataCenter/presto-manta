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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.presto.column.MantaPrestoColumn;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaPrestoRecordSetProvider implements ConnectorRecordSetProvider {
    private final String connectorId;
    private final MantaClient mantaClient;
    private final ObjectMapper objectMapper;

    @Inject
    public MantaPrestoRecordSetProvider(final MantaPrestoConnectorId connectorId,
                                        final MantaClient mantaClient,
                                        final ObjectMapper objectMapper) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.mantaClient = requireNonNull(mantaClient, "Manta client is null");
        this.objectMapper = requireNonNull(objectMapper, "object mapper is null");
    }

    @Override
    public RecordSet getRecordSet(final ConnectorTransactionHandle transactionHandle,
                                  final ConnectorSession session,
                                  final ConnectorSplit split,
                                  final List<? extends ColumnHandle> columns) {
        requireNonNull(split, "partitionChunk is null");
        MantaPrestoSplit mantaPrestoSplit = (MantaPrestoSplit) split;

        if (!mantaPrestoSplit.getConnectorId().equals(connectorId)) {
            throw new IllegalArgumentException("split is not for this connector");
        }

        ImmutableList.Builder<MantaPrestoColumn> handles = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            handles.add((MantaPrestoColumn) column);
        }

        return new MantaPrestoRecordSet(mantaPrestoSplit, handles.build(), mantaClient,
                objectMapper);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("connectorId", connectorId)
                .toString();
    }
}
