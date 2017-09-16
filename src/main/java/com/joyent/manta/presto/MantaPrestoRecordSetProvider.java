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
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaPrestoRecordSetProvider implements ConnectorRecordSetProvider {
    private final String connectorId;

    @Inject
    public MantaPrestoRecordSetProvider(MantaPrestoConnectorId connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public RecordSet getRecordSet(final ConnectorTransactionHandle transactionHandle,
                                  final ConnectorSession session,
                                  final ConnectorSplit split,
                                  final List<? extends ColumnHandle> columns) {
        requireNonNull(split, "partitionChunk is null");
        MantaPrestoSplit MantaPrestoSplit = (MantaPrestoSplit) split;

        if (!MantaPrestoSplit.getConnectorId().equals(connectorId)) {
            throw new IllegalArgumentException("split is not for this connector");
        }

        ImmutableList.Builder<MantaPrestoColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((MantaPrestoColumnHandle) handle);
        }

        return new MantaPrestoRecordSet(MantaPrestoSplit, handles.build());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("connectorId", connectorId)
                .toString();
    }
}
