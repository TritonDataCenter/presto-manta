/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.joyent.manta.client.MantaClient;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Manta specific implementation of {@link Connector} that acts to provide a
 * means to connect to Manta within the Presto API.
 */
public class MantaPrestoConnector implements Connector {
    private static final Logger log = Logger.get(MantaPrestoConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final MantaPrestoMetadata metadata;
    private final MantaPrestoSplitManager splitManager;
    private final MantaPrestoRecordSetProvider recordSetProvider;
    private final MantaClient mantaClient;

    @Inject
    public MantaPrestoConnector(final LifeCycleManager lifeCycleManager,
                                final MantaPrestoMetadata metadata,
                                final MantaPrestoSplitManager splitManager,
                                final MantaPrestoRecordSetProvider recordSetProvider,
                                final MantaClient mantaClient) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.mantaClient = mantaClient;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(final IsolationLevel isolationLevel,
                                                       final boolean readOnly) {
        return MantaPrestoTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(final ConnectorTransactionHandle transactionHandle) {
        return this.metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return this.splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return this.recordSetProvider;
    }

    @Override
    public void shutdown() {
        mantaClient.closeWithWarning();

        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            log.error(e, "Error shutting down Manta Presto connector");
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("lifeCycleManager", lifeCycleManager)
                .append("metadata", metadata)
                .append("splitManager", splitManager)
                .append("recordSetProvider", recordSetProvider)
                .toString();
    }
}
