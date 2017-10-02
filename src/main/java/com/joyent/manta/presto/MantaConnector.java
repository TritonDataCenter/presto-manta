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
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Manta specific implementation of {@link Connector} that acts to provide a
 * means to connect to Manta within the Presto API. This class must be shutdown
 * when it is no longer in use.
 *
 * @since 1.0.0
 */
public class MantaConnector implements Connector {
    private static final Logger LOG = LoggerFactory.getLogger(MantaConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final MantaMetadata metadata;
    private final MantaSplitManager splitManager;
    private final MantaRecordSetProvider recordSetProvider;
    private final MantaClient mantaClient;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param lifeCycleManager object that allows you to hook in lifecycle events
     * @param metadata object that provides metadata operations against schemas and tables
     * @param splitManager object that determines where to "split" data
     * @param recordSetProvider object that provides record sets based on table's columns
     * @param mantaClient object that allows for direct operations on Manta
     */
    @Inject
    public MantaConnector(final LifeCycleManager lifeCycleManager,
                          final MantaMetadata metadata,
                          final MantaSplitManager splitManager,
                          final MantaRecordSetProvider recordSetProvider,
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
        IsolationLevel.checkConnectorSupports(IsolationLevel.READ_COMMITTED,
                isolationLevel);

        return MantaTransactionHandle.INSTANCE;
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
            LOG.error("Error shutting down Manta Presto connector", e);
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
