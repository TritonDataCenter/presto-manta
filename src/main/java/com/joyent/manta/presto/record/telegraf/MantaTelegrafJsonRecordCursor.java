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
import com.facebook.presto.spi.block.SingleMapBlock;
import com.facebook.presto.spi.type.MapType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.CountingInputStream;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.record.json.MantaJsonRecordCursor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
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
        @SuppressWarnings("unchecked")
        final ObjectNode keyVals = (ObjectNode)row.get(field);
        final MapType mapType = (MapType)getType(field);

        final SingleMapBlock mapBlock = createSimpleMapBlock(mapType, keyVals);

        return mapBlock;
    }

    private Block createBlockWithValuesFromKeyValueBlock(Map<String, Long>[] maps)
    {
        List<String> keys = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        int[] offsets = new int[maps.length + 1];
        boolean[] mapIsNull = new boolean[maps.length];
        for (int i = 0; i < maps.length; i++) {
            Map<String, Long> map = maps[i];
            mapIsNull[i] = map == null;
            if (map == null) {
                offsets[i + 1] = offsets[i];
            }
            else {
                for (Map.Entry<String, Long> entry : map.entrySet()) {
                    keys.add(entry.getKey());
                    values.add(entry.getValue());
                }
                offsets[i + 1] = offsets[i] + map.size();
            }
        }
        return mapType(VARCHAR, BIGINT).createBlockFromKeyValue(mapIsNull, offsets, createStringsBlock(keys), createLongsBlock(values));
    }

    private static SingleMapBlock createSimpleMapBlock(final MapType mapType,
                                                       final ObjectNode keyvals) {
        Iterator<Map.Entry<String, JsonNode>> itr = keyvals.fields();

        while (itr.hasNext()) {

        }

        return mapType.createBlockFromKeyValue()
    }
}
