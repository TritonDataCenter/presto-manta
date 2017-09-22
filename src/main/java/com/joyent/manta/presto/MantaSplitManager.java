/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.presto.exceptions.MantaPrestoUnexpectedClass;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaSplitManager implements ConnectorSplitManager {
    private final String connectorId;

    @Inject
    public MantaSplitManager(final MantaConnectorId connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public ConnectorSplitSource getSplits(final ConnectorTransactionHandle handle,
                                          final ConnectorSession session,
                                          final ConnectorTableLayoutHandle layout) {
        if (!layout.getClass().equals(MantaTableLayoutHandle.class)) {
            throw new MantaPrestoUnexpectedClass(MantaTableLayoutHandle.class,
                    handle.getClass());
        }

        MantaTableLayoutHandle layoutHandle = (MantaTableLayoutHandle)layout;
        MantaSchemaTableName tableName = layoutHandle.getTableName();

        MantaSplit split = new MantaSplit(connectorId,
                tableName.getSchemaName(), tableName.getTableName(),
                tableName.getObjectPath());

        // TODO: We only map one table to one file for now

        return new FixedSplitSource(ImmutableList.of(split));
    }
}
