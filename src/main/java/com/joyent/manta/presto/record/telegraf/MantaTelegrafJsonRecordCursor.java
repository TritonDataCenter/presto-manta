/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.telegraf;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.MapBlock;
import com.facebook.presto.spi.block.SingleMapBlock;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.CountingInputStream;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.record.json.MantaJsonRecordCursor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * {@link RecordCursor} implementation that reads each new line of JSON into
 * a single row and maps the columns to Telegraf compatible data types.
 *
 * @since 1.0.0
 */
public class MantaTelegrafJsonRecordCursor extends MantaJsonRecordCursor {
    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param columns list of columns in table
     * @param objectPath path to object in Manta
     * @param totalBytes total number of bytes in source object
     * @param countingStream input stream that counts the number of bytes processed
     * @param streamingReader streaming json deserialization reader
     */
    public MantaTelegrafJsonRecordCursor(final List<MantaColumn> columns,
                                         final String objectPath,
                                         final Long totalBytes,
                                         final CountingInputStream countingStream,
                                         final ObjectReader streamingReader) {
        super(columns, objectPath, totalBytes, countingStream, streamingReader);
    }

    @Override
    public Object getObject(final int field) {
        Type type = getType(field);

        if (type.equals(MantaTelegrafColumnLister.STRING_MAP)) {
            @SuppressWarnings("unchecked")
            final ObjectNode keyVals = (ObjectNode) getRow().get(field);
            final MapType mapType = (MapType) getType(field);

            return createSimpleMapBlock(mapType, keyVals);
        }

        return super.getObject(field);
    }

    @Override
    protected long getTimestampFromLong(final JsonNode value) {
        final long milliSecondConversionFactor = 1_000;
        return super.getTimestampFromLong(value) * milliSecondConversionFactor;
    }

    private static SingleMapBlock createSimpleMapBlock(final MapType mapType,
                                                       final ObjectNode objectNode) {
        final int length = objectNode.size();
        final Iterator<Map.Entry<String, JsonNode>> itr = objectNode.fields();
        final int[] offsets = new int[] {0, length};
        final boolean[] mapIsNeverNull = new boolean[] {true};

        final String[] keys = new String[length];
        final String[] vals = new String[length];

        for (int i = 0; itr.hasNext(); i++) {
            final Map.Entry<String, JsonNode> entry = itr.next();
            keys[i] = entry.getKey();
            vals[i] = entry.getValue().asText();
        }

        final MapBlock mapBlock = mapType.createBlockFromKeyValue(mapIsNeverNull, offsets,
                createStringsBlock(keys), createStringsBlock(vals));
        @SuppressWarnings("unchecked")
        final SingleMapBlock singleMapBlock = (SingleMapBlock)mapBlock.getObject(0, Block.class);

        return singleMapBlock;
    }

    private static Block createStringsBlock(final String[] values) {
        final int expectedEntries = 100;
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
}
