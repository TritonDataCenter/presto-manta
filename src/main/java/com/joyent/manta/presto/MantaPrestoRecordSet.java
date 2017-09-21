/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.presto.column.MantaPrestoColumn;
import com.joyent.manta.presto.record.json.MantaPrestoJsonRecordCursor;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaPrestoRecordSet implements RecordSet {
    private final List<MantaPrestoColumn> columns;
    private final List<Type> columnTypes;
    private final String objectPath;
    private final MantaClient mantaClient;
    private final ObjectMapper objectMapper;

    public MantaPrestoRecordSet(final MantaPrestoSplit split,
                                final List<MantaPrestoColumn> columns,
                                final MantaClient mantaClient,
                                final ObjectMapper objectMapper) {
        requireNonNull(split, "split is null");
        this.columns = requireNonNull(columns, "column handles is null");
        this.mantaClient = requireNonNull(mantaClient, "Manta client is null");
        this.objectMapper = requireNonNull(objectMapper, "object mapper is null");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (MantaPrestoColumn column : columns) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();

        this.objectPath = split.getObjectPath();
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new MantaPrestoJsonRecordCursor(columns, objectPath, mantaClient, objectMapper);
    }
}
