/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.json;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.io.CountingInputStream;
import com.joyent.manta.presto.column.MantaPrestoColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
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

/**
 *
 */
public class MantaPrestoJsonRecordCursor implements RecordCursor {
    private static final Logger LOG = LoggerFactory.getLogger(MantaPrestoJsonRecordCursor.class);

    private final ObjectMapper objectMapper;

    private final List<MantaPrestoColumn> columns;
    private final long totalBytes;
    private long lines = 0L;
    private Long readTimeStartNanos = null;

    private final String objectPath;
    private final CountingInputStream countingStream;
    private final Scanner scanner;

    private Map<Integer, JsonNode> row;

    public MantaPrestoJsonRecordCursor(final List<MantaPrestoColumn> columns,
                                       final String objectPath,
                                       final long totalBytes,
                                       final CountingInputStream countingStream,
                                       final Charset charset,
                                       final ObjectMapper objectMapper) {
        this.columns = columns;
        this.objectPath = objectPath;
        this.objectMapper = objectMapper;
        this.totalBytes = totalBytes;
        this.countingStream = countingStream;
        this.scanner = new Scanner(this.countingStream, charset.name());
    }

    @Override
    public long getTotalBytes() {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes() {
        return countingStream.getCount();
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
            me.setContextValue("objectPath", objectPath);
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
        final JsonNode node = row.get(field);
        final String text;

        /* We determine if we have a text node and render it as text for the
         * typical case. For JSON objects, although they are identified as
         * the JSON type within Presto, they are processed as a Slice from a
         * plugin's perspective, so we detect if a node is an object and just
         * render it as JSON text and return.
         */
        if (node.isTextual()) {
            text = node.asText();
        } else if (node.isObject()) {
            text = node.toString();
        } else {
            String msg = "Unsupported type passed as slice";
            MantaPrestoIllegalArgumentException me = new MantaPrestoIllegalArgumentException(msg);
            me.setContextValue("objectPath", objectPath);
            me.setContextValue("fieldNumber", field);
            me.setContextValue("fieldMappings", Joiner.on("->").join(row.entrySet()));
            me.setContextValue("node", node);
            throw me;
        }

        return Slices.utf8Slice(text);
    }

    @Override
    public Object getObject(final int field) {
        return row.get(field);
    }

    @Override
    public boolean isNull(final int field) {
        final JsonNode node = row.get(field);

        if (node == null) {
            String msg = "Invalid field number specified";
            MantaPrestoIllegalArgumentException me = new MantaPrestoIllegalArgumentException(msg);
            me.setContextValue("objectPath", objectPath);
            me.setContextValue("fieldNumber", field);
            me.setContextValue("fieldMappings", Joiner.on("->").join(row.entrySet()));
            throw me;
        }

        return node.isNull();
    }

    @Override
    public void close() {
        scanner.close();
        Closeables.closeQuietly(countingStream);
    }

    private Map<Integer, JsonNode> mapOrdinalToNode(final ObjectNode object) {
        ImmutableMap.Builder<Integer, JsonNode> map = new ImmutableMap.Builder<>();

        int count = 0;
        for (MantaPrestoColumn column : columns) {
            map.put(count++, object.get(column.getName()));
        }

        return map.build();
    }
}
