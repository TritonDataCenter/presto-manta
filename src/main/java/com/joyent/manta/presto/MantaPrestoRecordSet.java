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
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;

import java.net.MalformedURLException;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaPrestoRecordSet implements RecordSet {
    private final List<MantaPrestoColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final ByteSource byteSource;

    public MantaPrestoRecordSet(final MantaPrestoSplit split,
                                final List<MantaPrestoColumnHandle> columnHandles) {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (MantaPrestoColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        try {
            byteSource = Resources.asByteSource(split.getUri().toURL());
        } catch (MalformedURLException e) {
            String msg = "Bad URL received from MantaPrestoSplit instance";
            MantaPrestoRuntimeException re = new MantaPrestoRuntimeException(msg, e);
            re.setContextValue("URL", split.getUri());

            throw re;
        }
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new MantaPrestoRecordCursor(columnHandles, byteSource);
    }
}
