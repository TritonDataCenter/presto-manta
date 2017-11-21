/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.json;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.MapBlock;
import com.facebook.presto.spi.block.SingleMapBlock;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.joyent.manta.presto.MantaCountingInputStream;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoExceptionUtils;
import com.joyent.manta.presto.exceptions.MantaPrestoFileFormatException;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import com.joyent.manta.presto.types.MapStringType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.SocketTimeoutException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * {@link RecordCursor} implementation that reads each new line of JSON into
 * a single row and columns.
 *
 * @since 1.0.0
 */
public class MantaJsonRecordCursor implements RecordCursor {
    private static final Logger LOG = LoggerFactory.getLogger(MantaJsonRecordCursor.class);

    private static final Function<Map.Entry<String, JsonNode>, String> JSON_STRING_VALUE_EXTRACT_FUNCTION =
            entry -> entry.getValue().asText();

    private static final Function<Map.Entry<String, JsonNode>, Double> JSON_DOUBLE_VALUE_EXTRACT_FUNCTION =
            entry -> entry.getValue().doubleValue();

    private final List<MantaColumn> columns;
    private Long totalBytes = null;
    private long lines = 0L;
    private int retries = 0;
    private Long readTimeStartNanos = null;
    private Map<Integer, JsonNode> row;
    private MantaCountingInputStream countingStream;
    private MappingIterator<ObjectNode> lineItr;

    private final Supplier<MantaCountingInputStream> streamRecreator;
    private final String objectPath;
    private final ObjectReader streamingReader;

    private final Map<String, DateTimeFormatter> dateFormats = new HashMap<>();

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param streamRecreator function used to recreate the underlying stream providing the JSON data
     * @param columns list of columns in table
     * @param objectPath path to object in Manta
     * @param totalBytes total number of bytes in source object
     * @param countingStream input stream that counts the number of bytes processed
     * @param streamingReader streaming json deserialization reader
     */
    public MantaJsonRecordCursor(final Supplier<MantaCountingInputStream> streamRecreator,
                                 final List<MantaColumn> columns,
                                 final String objectPath,
                                 final Long totalBytes,
                                 final MantaCountingInputStream countingStream,
                                 final ObjectReader streamingReader) {
        this.streamRecreator = streamRecreator;
        this.columns = columns;
        this.objectPath = objectPath;
        this.totalBytes = totalBytes;
        this.streamingReader = streamingReader;
        this.countingStream = countingStream;
        this.lineItr = buildLineIteratorFromStream(countingStream);
    }

    private MappingIterator<ObjectNode> buildLineIteratorFromStream(final MantaCountingInputStream in) {
        try {
            return streamingReader.readValues(in);
        } catch (JsonParseException e) {
            String msg = "Can't parse input data as valid JSON";
            MantaPrestoFileFormatException me = new MantaPrestoFileFormatException(msg, e);
            me.setContextValue("objectPath", objectPath);
            me.setContextValue("jsonPayload", e.getRequestPayloadAsString());
            me.setContextValue("bytePosition", in.getCount());

            if (e.getProcessor() != null) {
                final JsonParser parser = e.getProcessor();
                me.setContextValue("parser", parser.getClass());
                if (parser.getInputSource() != null) {
                    me.setContextValue("inputSource", parser.getInputSource().getClass());
                }
            }

            throw me;
        } catch (IOException e) {
            String msg = "Unable to create a line iterator JSON parser";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.setContextValue("objectPath", objectPath);
            me.setContextValue("bytePosition", in.getCount());
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
        return getColumn(field).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        return advanceNextPosition(-1L);
    }

    /**
     * Advances the cursor to the next position.
     *
     * @param lineToAdvanceTo if -1 or less, it advances to the next available line
     *                        otherwise, it advances to the line specified
     * @return true if a line is available, otherwise false if not available
     */
    private boolean advanceNextPosition(final long lineToAdvanceTo) {
        if (readTimeStartNanos == null) {
            readTimeStartNanos = System.nanoTime();
        }

        if (!lineItr.hasNext()) {
            if (totalBytes == null) {
                totalBytes = countingStream.getCount();
            }
            return false;
        }

        // Only increment the line count if we aren't skipping lines
        if (lineToAdvanceTo < 0) {
            lines++;
        }

        try {
            // Skip lines until we hit the specified line
            for (int i = 0; i < lineToAdvanceTo; i++) {
                lineItr.next();
            }

            ObjectNode node = lineItr.next();
            row = mapOrdinalToNode(node);
        } catch (RuntimeException e) {
            MantaPrestoRuntimeException me = new MantaPrestoRuntimeException(e);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(countingStream, me);

            me.setContextValue("line", lines);
            me.setContextValue("lineToAdvanceTo", lineToAdvanceTo);
            me.setContextValue("retries", retries);

            if (lineItr.getCurrentLocation() != null) {
                me.addContextValue("jsonIteratorByteOffset",
                        lineItr.getCurrentLocation().getByteOffset());
            }

            me.addContextValue("streamBytePosition", countingStream.getCount());

            /* When we encounter a socket read time out, we attempt to open up
             * a new connection at the position of the current line that
             * hasn't been successfully read. We do this because there are
             * spurious network conditions in which we have no proper way of
             * recovering from without doing a retry.
             */
            if (e.getCause() != null
                    && e.getCause().getClass().equals(SocketTimeoutException.class)
                    && streamRecreator != null
                    && lineToAdvanceTo < 0) {
                LOG.info("Retrying download for object due to socket timeout", me);
                Closeables.closeQuietly(countingStream);
                countingStream = streamRecreator.get();
                lineItr = buildLineIteratorFromStream(countingStream);

                /* We don't allow more than a single retry because if you are
                 * getting many socket timeouts it is indicative of a failure
                 * in which we want to error on.
                 */
                retries++;

                /* We download the data file again and skip lines until we reach
                 * the current line. We pass in the current line minus one because
                 * lines was incremented before the cursor was advanced we did this
                 * because it allowed us to return the correct position for errors.
                 * However, it does make it a bit unclear when skipping lines
                 * as we do below.
                 */
                return advanceNextPosition(lines - 1);
            }

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
        final MantaColumn column = getColumn(field);
        final JsonNode value = row.get(field);
        final String type = column.getType().getTypeSignature().getBase();

        switch (type) {
            case "timestamp":
                return getTimestampFromLong(value);
            case "date":
                return getDate(value, column);
            default:
                return value.asLong();
        }
    }

    /**
     * Reads the long value from a json numeric property as a
     * long representing a timestamp. By default we read as
     * epoch milliseconds. Extenders of this class may choose
     * to convert from epoch seconds.
     *
     * @param value json node to read long value from
     * @return long representing epoch milliseconds
     */
    protected long getTimestampFromLong(final JsonNode value) {
        return value.longValue();
    }

    /**
     * Presto attempts to read date types into long values.
     *
     * @param value parsed JSON node value
     * @param column column associated with node
     * @return date as represented in the number of milliseconds from
     *         1970-01-01T00:00:00 in UTC but time must be midnight in the
     *         local time zone
     */
    protected long getDate(final JsonNode value, final MantaColumn column) {
        final String pattern = StringUtils.substringAfter(
                column.getExtraInfo(), "date ");

        final DateTimeFormatter format = dateFormats.computeIfAbsent(
                pattern, s -> DateTimeFormatter.ofPattern(pattern));

        try {
            final LocalDate date = LocalDate.parse(value.asText(), format);
            return date.toEpochDay();
        } catch (DateTimeParseException e) {
            String msg = "There was a problem parsing a date value";
            MantaPrestoFileFormatException me = new MantaPrestoFileFormatException(msg);
            me.setContextValue("column", column.getName());
            me.setContextValue("columnExtraInfo", column.getExtraInfo());
            me.setContextValue("input", value.toString());
            me.setContextValue("datePattern", pattern);
            me.setContextValue("line", lines);
            me.setContextValue("objectPath", objectPath);
            throw me;
        }
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

            if (columns != null) {
                me.setContextValue("column", getColumn(field));
            }

            me.setContextValue("fieldNumber", field);
            me.setContextValue("fieldMappings", Joiner.on("->").join(row.entrySet()));
            me.setContextValue("node", node);
            throw me;
        }

        return Slices.utf8Slice(text);
    }

    @Override
    public Object getObject(final int field) {
        Type type = getType(field);

        if (type instanceof MapType) {
            @SuppressWarnings("unchecked")
            final ObjectNode keyVals = (ObjectNode) getRow().get(field);
            final MapType mapType = (MapType) getType(field);

            if (type.equals(MapStringType.MAP_STRING_STRING)) {
                return createSimpleMapBlockWithStringValues(mapType, keyVals);
            } else if (type.equals(MapStringType.MAP_STRING_DOUBLE)) {
                return createSimpleMapBlockWithDoubleValues(mapType, keyVals);
            }
        }

        String column = getColumn(field).getName();
        String template = "getObject not supported for type [column=%s,field=%d,type=%s]";
        String msg = String.format(template, column, field, type);
        throw new UnsupportedOperationException(msg);
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
            final String columnName = Objects.requireNonNull(column.getName(),
                    "Column name is null");
            final JsonNode node = object.get(columnName);

            if (node == null) {
                String msg = "No column found with the specified name";
                MantaPrestoIllegalArgumentException e = new MantaPrestoIllegalArgumentException(msg);
                e.setContextValue("columnName", columnName);
                String fields = Joiner.on(',').join(object.fieldNames());
                e.setContextValue("fields", fields);

                throw e;
            }

            map.put(count++, node);
        }

        return map.build();
    }

    protected Map<Integer, JsonNode> getRow() {
        return row;
    }

    /**
     * Gets the column corresponding to specified field number.
     *
     * @param field field to query for column information
     * @return column corresponding to field
     */
    protected MantaColumn getColumn(final int field) {
        try {
            return columns.get(field);
        } catch (IndexOutOfBoundsException e) {
            String msg = String.format("No column maps to field [field=%d]", field);
            throw new IllegalArgumentException(msg, e);
        }
    }

    /**
     * Creates an instance of {@link SingleMapBlock} so that a Map type can
     * be returned to Presto as the value of a column within a row. This
     * method creates maps with String keys and String values.
     *
     * @param mapType presto map type with key and value types defined
     * @param objectNode JSON object node to parse into Presto Map type
     * @return a block instance populated with the key/values of the JSON object
     */
    protected static SingleMapBlock createSimpleMapBlockWithStringValues(final MapType mapType,
                                                                         final ObjectNode objectNode) {
        return createSimpleMapBlock(mapType, objectNode, String.class, JSON_STRING_VALUE_EXTRACT_FUNCTION,
                MantaJsonRecordCursor::createStringsBlock);
    }

    /**
     * Creates an instance of {@link SingleMapBlock} so that a Map type can
     * be returned to Presto as the value of a column within a row. This
     * method creates maps with String keys and Double values.
     *
     * @param mapType presto map type with key and value types defined
     * @param objectNode JSON object node to parse into Presto Map type
     * @return a block instance populated with the key/values of the JSON object
     */
    protected static SingleMapBlock createSimpleMapBlockWithDoubleValues(final MapType mapType,
                                                                         final ObjectNode objectNode) {
        return createSimpleMapBlock(mapType, objectNode, Double.class, JSON_DOUBLE_VALUE_EXTRACT_FUNCTION,
                MantaJsonRecordCursor::createDoublesBlock);
    }

    /**
     * Creates an instance of {@link SingleMapBlock} so that a Map type can
     * be returned to Presto as the value of a column within a row.
     *
     * @param mapType presto map type with key and value types defined
     * @param objectNode JSON object node to parse into Presto Map type
     * @param valueClass class of the value returned by the map being created
     * @param extractFunction function used to extract a single value from the passed JSON object
     * @param valueBlockCreateFunction function used to create the block for the map values returned to Presto
     * @param <VALUE> type of value returned by the Presto map
     * @return a block instance populated with the key/values of the JSON object
     */
    @SuppressWarnings("unchecked")
    protected static <VALUE> SingleMapBlock createSimpleMapBlock(final MapType mapType,
                                                                 final ObjectNode objectNode,
                                                                 final Class<VALUE> valueClass,
                                                                 final Function<Map.Entry<String, JsonNode>, VALUE> extractFunction,
                                                                 final Function<VALUE[], Block> valueBlockCreateFunction) {
        final int length = objectNode.size();
        final Iterator<Map.Entry<String, JsonNode>> itr = objectNode.fields();
        final int[] offsets = new int[] {0, length};
        final boolean[] mapIsNeverNull = new boolean[] {true};

        final String[] keys = new String[length];
        final VALUE[] vals = (VALUE[])Array.newInstance(valueClass, length);

        for (int i = 0; itr.hasNext(); i++) {
            final Map.Entry<String, JsonNode> entry = itr.next();
            keys[i] = entry.getKey();
            vals[i] = extractFunction.apply(entry);
        }

        final MapBlock mapBlock = mapType.createBlockFromKeyValue(mapIsNeverNull, offsets,
                createStringsBlock(keys), valueBlockCreateFunction.apply(vals));
        @SuppressWarnings("unchecked")
        final SingleMapBlock singleMapBlock = (SingleMapBlock)mapBlock.getObject(0, Block.class);

        return singleMapBlock;
    }

    /**
     * Creates a {@link Block} containing multiple String values.
     * @param values Strings to add to block
     * @return Block populated with Strings
     */
    protected static Block createStringsBlock(final String[] values) {
        final int expectedEntries = values.length;
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), expectedEntries);

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            } else {
                VARCHAR.writeString(builder, value);
            }
        }

        return builder.build();
    }

    /**
     * Creates a {@link Block} containing multiple Double values.
     * @param values Strings to add to block
     * @return Block populated with Doubles
     */
    protected static Block createDoublesBlock(final Double[] values) {
        final int expectedEntries = values.length;
        BlockBuilder builder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), expectedEntries);

        for (double value : values) {
            DOUBLE.writeDouble(builder, value);
        }

        return builder.build();
    }
}
