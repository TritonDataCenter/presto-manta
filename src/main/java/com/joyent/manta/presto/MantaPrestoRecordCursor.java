/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.io.CountingInputStream;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.presto.column.MantaPrestoColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoExceptionUtils;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 *
 */
public class MantaPrestoRecordCursor implements RecordCursor {
    private static final Logger LOG = LoggerFactory.getLogger(MantaPrestoRecordCursor.class);

    private final ObjectMapper objectMapper;

    private final List<MantaPrestoColumn> columns;
    private final long totalBytes;
    private long lines = 0L;
    private Long readTimeStartNanos = null;
    private List<String> fields;

    private final String objectPath;
    private final CountingInputStream countingInputStream;
    private final MantaObjectInputStream mantaObjectInputStream;
    private final Scanner scanner;

    private Map<Integer, JsonNode> row;

    public MantaPrestoRecordCursor(final List<MantaPrestoColumn> columns,
                                   final String objectPath,
                                   final MantaClient mantaClient,
                                   final ObjectMapper objectMapper) {
        this.columns = columns;
        this.objectPath = objectPath;
        this.objectMapper = objectMapper;

        try {
            this.mantaObjectInputStream = mantaClient.getAsInputStream(objectPath);
            this.totalBytes = mantaObjectInputStream.getContentLength();
        } catch (IOException e) {
            String msg = "There was a problem opening a connection to Manta";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.addContextValue("objectPath", objectPath);
            throw me;
        }

        this.countingInputStream = new CountingInputStream(this.mantaObjectInputStream);
        String contentType = mantaObjectInputStream.getContentType();
        Charset charset = MantaPrestoUtils.parseCharset(contentType, UTF_8);
        this.scanner = new Scanner(countingInputStream, charset.name());
    }

    @Override
    public long getTotalBytes() {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes() {
        return countingInputStream.getCount();
    }

    @Override
    public long getReadTimeNanos() {
        if (readTimeStartNanos == null) {
            return 0L;
        }

        return Math.abs(readTimeStartNanos - System.nanoTime());
    }

    @Override
    public Type getType(final int field) {
        checkArgument(field < columns.size(), "Invalid field index");
        return columns.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (readTimeStartNanos == null) {
            readTimeStartNanos = System.nanoTime();
        }

        if (!scanner.hasNextLine()) {
            return false;
        }
        String line = scanner.nextLine();
        lines++;

        try {
            ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
            row = mapOrdinalToNode(node);
        } catch (IOException e) {
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(e);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(mantaObjectInputStream, me);
            me.addContextValue("line", lines);

            LOG.info("Unable to parse line", me);
        }

        return true;
    }

    @Override
    public boolean getBoolean(final int field) {
        return row.get(field).asBoolean();
    }

    @Override
    public long getLong(final int field) {
        return row.get(field).asLong();
    }

    @Override
    public double getDouble(final int field) {
        return row.get(field).asDouble();
    }

    @Override
    public Slice getSlice(final int field) {
        return Slices.utf8Slice(row.get(field).asText());
    }

    @Override
    public Object getObject(final int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(final int field) {
        checkArgument(field < columns.size(), "Invalid field index");

        return row.get(field).isNull() || Strings.isNullOrEmpty(row.get(field).asText());
    }

    @Override
    public void close() {
        Closeables.closeQuietly(countingInputStream);
        Closeables.closeQuietly(mantaObjectInputStream);
    }

    private static Map<Integer, JsonNode> mapOrdinalToNode(final ObjectNode object) {
        ImmutableMap.Builder<Integer, JsonNode> map = new ImmutableMap.Builder<>();

        int count = 0;
        for (JsonNode node : object) {
            map.put(count++, node);
        }

        return map.build();
    }
}
