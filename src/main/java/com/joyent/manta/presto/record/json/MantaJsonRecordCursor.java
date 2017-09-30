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
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.io.CountingInputStream;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link RecordCursor} implementation that reads each new line of JSON into
 * a single row and columns.
 *
 * @since 1.0.0
 */
public class MantaJsonRecordCursor implements RecordCursor {
    private static final Logger LOG = LoggerFactory.getLogger(MantaJsonRecordCursor.class);

    private final List<MantaColumn> columns;
    private Long totalBytes = null;
    private long lines = 0L;
    private Long readTimeStartNanos = null;

    private final String objectPath;
    private final CountingInputStream countingStream;
    private final MappingIterator<ObjectNode> lineItr;

    private Map<Integer, JsonNode> row;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param columns list of columns in table
     * @param objectPath path to object in Manta
     * @param totalBytes total number of bytes in source object
     * @param countingStream input stream that counts the number of bytes processed
     * @param mapper Jackson JSON serialization / deserialization object
     */
    public MantaJsonRecordCursor(final List<MantaColumn> columns,
                                 final String objectPath,
                                 final Long totalBytes,
                                 final CountingInputStream countingStream,
                                 final ObjectMapper mapper) {
        this.columns = columns;
        this.objectPath = objectPath;
        this.totalBytes = totalBytes;
        this.countingStream = countingStream;

        try {
            this.lineItr = mapper.readerFor(ObjectNode.class).readValues(countingStream);
        } catch (IOException e) {
            String msg = "Unable to create a line iterator JSON parser";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.setContextValue("objectPath", objectPath);
            throw me;
        }
    }

    @Override
    public long getTotalBytes() {
        if (totalBytes == null) {
            return -1L;
        }

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

        if (!lineItr.hasNext()) {
            if (totalBytes == null) {
                totalBytes = countingStream.getCount();
            }
            return false;
        }

        lines++;

        try {
            ObjectNode node = lineItr.next();
            row = mapOrdinalToNode(node);
        } catch (RuntimeException e) {
            MantaPrestoRuntimeException me = new MantaPrestoRuntimeException(e);
            me.setContextValue("objectPath", objectPath);
            me.addContextValue("line", lines);

            throw me;
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
        try {
            lineItr.close();
        } catch (IOException e) {
            LOG.info("Error closing JSON line parser", e);
        }

        Closeables.closeQuietly(countingStream);
    }

    private Map<Integer, JsonNode> mapOrdinalToNode(final ObjectNode object) {
        ImmutableMap.Builder<Integer, JsonNode> map = new ImmutableMap.Builder<>();

        int count = 0;
        for (MantaColumn column : columns) {
            map.put(count++, object.get(column.getName()));
        }

        return map.build();
    }
}
