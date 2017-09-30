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
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.exceptions.MantaPrestoUnexpectedClass;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;

import javax.inject.Inject;
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

        MantaTableLayoutHandle layoutHandle = (MantaTableLayoutHandle)layout;
        MantaSchemaTableName tableName = layoutHandle.getTableName();
        MantaLogicalTable table = tableName.getTable();

        Stream<MantaObject> objectStream = mantaClient.find(table.getRootPath(),
                table.directoryFilter())
                .filter(table.filter())
                .filter(obj -> !obj.isDirectory())
                .filter(obj -> !obj.getPath().endsWith(TABLE_DEFINITION_FILENAME));

        return new MantaStreamingSplitSource(connectorId, tableName.getSchemaName(),
                tableName.getTableName(), table.getDataFileType(), objectStream);
    }
}
