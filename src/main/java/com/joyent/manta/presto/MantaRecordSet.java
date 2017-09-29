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
import com.google.common.io.CountingInputStream;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoExceptionUtils;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import com.joyent.manta.presto.record.json.MantaJsonRecordCursor;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaRecordSet implements RecordSet {
    private final List<MantaColumn> columns;
    private final List<Type> columnTypes;
    private final String objectPath;
    private final MantaClient mantaClient;
    private final ObjectMapper objectMapper;
    private final MantaDataFileType dataFileType;

    public MantaRecordSet(final MantaSplit split,
                          final List<MantaColumn> columns,
                          final MantaClient mantaClient,
                          final ObjectMapper objectMapper) {
        requireNonNull(split, "split is null");
        this.columns = requireNonNull(columns, "column handles is null");
        this.mantaClient = requireNonNull(mantaClient, "Manta client is null");
        this.objectMapper = requireNonNull(objectMapper, "object mapper is null");
        this.dataFileType = requireNonNull(split.getDataFileType(), "data file type is null");
        this.objectPath = requireNonNull(split.getObjectPath(), "object path is null");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (MantaColumn column : columns) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        final MantaObjectInputStream mantaInputStream;
        final InputStream in;

        try {
            mantaInputStream = mantaClient.getAsInputStream(objectPath);
            in = MantaCompressionType.wrapMantaStreamIfCompressed(mantaInputStream);
        } catch (IOException e) {
            String msg = "There was a problem opening a connection to Manta";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.addContextValue("objectPath", objectPath);
            throw me;
        }

        long totalBytes = mantaInputStream.getContentLength();
        CountingInputStream cin = new CountingInputStream(in);

        switch (dataFileType) {
            case NDJSON:
                return new MantaJsonRecordCursor(columns, objectPath,
                        totalBytes, cin, objectMapper);
            default:
                String msg = "Can't create cursor for unsupported file type";
                MantaPrestoIllegalArgumentException me = new MantaPrestoIllegalArgumentException(msg);
                me.setContextValue("dataFileType", dataFileType);
                MantaPrestoExceptionUtils.annotateMantaObjectDetails(mantaInputStream, me);
                throw me;
        }

    }
}