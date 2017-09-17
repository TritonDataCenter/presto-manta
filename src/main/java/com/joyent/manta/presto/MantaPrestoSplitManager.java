/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaPrestoSplitManager implements ConnectorSplitManager {
    private final String connectorId;
    private final MantaPrestoClient MantaPrestoClient;

    @Inject
    public MantaPrestoSplitManager(final MantaPrestoConnectorId connectorId,
                                   final MantaPrestoClient MantaPrestoClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.MantaPrestoClient = requireNonNull(MantaPrestoClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(final ConnectorTransactionHandle handle,
                                          final ConnectorSession session,
                                          final ConnectorTableLayoutHandle layout) {
        MantaPrestoTableLayoutHandle layoutHandle = (MantaPrestoTableLayoutHandle) layout;
        MantaPrestoTableHandle tableHandle = layoutHandle.getTable();
//        MantaPrestoTable table = MantaPrestoClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());


        List<ConnectorSplit> splits = new ArrayList<>();
//        for (URI uri : table.getSources()) {
//            splits.add(new MantaPrestoSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), uri));
//        }
//        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
