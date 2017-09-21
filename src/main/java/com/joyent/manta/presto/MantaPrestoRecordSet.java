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
import com.joyent.manta.presto.column.MantaPrestoColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoExceptionUtils;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import com.joyent.manta.presto.record.json.MantaPrestoJsonRecordCursor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
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
        final MantaObjectInputStream in;

        try {
            in = mantaClient.getAsInputStream(objectPath);
        } catch (IOException e) {
            String msg = "There was a problem opening a connection to Manta";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.addContextValue("objectPath", objectPath);
            throw me;
        }

        String contentType = in.getContentType();
        Charset charset = MantaPrestoUtils.parseCharset(contentType, UTF_8);
        long totalBytes = in.getContentLength();
        CountingInputStream cin = new CountingInputStream(in);

        MantaPrestoFileType type = MantaPrestoFileType.determineFileType(in);

        switch (type) {
            case LDJSON:
                return new MantaPrestoJsonRecordCursor(columns, objectPath,
                        totalBytes, cin, charset, objectMapper);
            default:
                String msg = "Can't create cursor for unsupported file type";
                MantaPrestoIllegalArgumentException me = new MantaPrestoIllegalArgumentException(msg);
                me.setContextValue("type", type);
                MantaPrestoExceptionUtils.annotateMantaObjectDetails(in, me);
                throw me;
        }

    }
}
