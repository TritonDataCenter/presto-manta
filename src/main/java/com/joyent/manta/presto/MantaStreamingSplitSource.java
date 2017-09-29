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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**

 */
public class MantaStreamingSplitSource implements ConnectorSplitSource {

    private final Iterator<MantaSplit> iterator;
    private final Stream<MantaObject> backingStream;

    public MantaStreamingSplitSource(final String connectorId,
                                     final String schemaName,
                                     final String tableName,
                                     final MantaDataFileType dataFileType,
                                     final Stream<MantaObject> backingStream) {
        this.backingStream = backingStream;
        this.iterator = backingStream
                .map(obj -> new MantaSplit(connectorId, schemaName, tableName, obj.getPath(), dataFileType))
                .iterator();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(final int maxSize) {
        return CompletableFuture.supplyAsync(() -> {
            final ImmutableList.Builder<ConnectorSplit> list = new ImmutableList.Builder<>();

            for (int i = 0; i < maxSize && iterator.hasNext(); i++) {
                list.add(iterator.next());
            }

            return list.build();
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
