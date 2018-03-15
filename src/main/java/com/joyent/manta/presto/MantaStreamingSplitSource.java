/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * {@link ConnectorSplitSource} implementation that takes a stream of Manta
 * objects and turns them into asynchronous splittable units and returns them.
 *
 * @since 1.0.0
 */
public class MantaStreamingSplitSource implements ConnectorSplitSource {
    private static final Logger LOG = LoggerFactory.getLogger(MantaStreamingSplitSource.class);

    private final Iterator<MantaSplit> iterator;
    private final Stream<MantaObject> backingStream;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param connectorId presto connection id object for debugging
     * @param schemaName schema as defined in Presto catalog configuration
     * @param tableName table as defined in table definition file
     * @param dataFileType data type of all objects in table
     * @param backingStream stream of objects that will be processed into splits
     * @param filePartitionPredicate partitioning scheme used to partition by filename
     * @param dirPartitionPredicate partitioning scheme used to partition by directory
     */
    public MantaStreamingSplitSource(final String connectorId,
                                     final String schemaName,
                                     final String tableName,
                                     final MantaDataFileType dataFileType,
                                     final Stream<MantaObject> backingStream,
                                     final MantaSplitPartitionPredicate filePartitionPredicate,
                                     final MantaSplitPartitionPredicate dirPartitionPredicate) {
        this.backingStream = backingStream;
        this.iterator = backingStream
                .map(obj -> new MantaSplit(connectorId, schemaName, tableName,
                        obj.getPath(), dataFileType, filePartitionPredicate, dirPartitionPredicate))
                .iterator();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(final int maxSize) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                final ImmutableList.Builder<ConnectorSplit> list = new ImmutableList.Builder<>();

                for (int i = 0; i < maxSize && iterator.hasNext(); i++) {
                    list.add(iterator.next());
                }

                return list.build();
            } catch (Exception e) {
                LOG.error("Error getting next batch of ConnectorSplit objects", e);
                throw e;
            }
        });
    }

    @Override
    public void close() {
        backingStream.close();
    }

    @Override
    public boolean isFinished() {
        return !iterator.hasNext();
    }
}
