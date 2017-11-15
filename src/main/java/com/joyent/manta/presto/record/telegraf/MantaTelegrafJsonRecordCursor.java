/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.telegraf;

import com.facebook.presto.spi.RecordCursor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.io.CountingInputStream;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.record.json.MantaJsonRecordCursor;

import java.util.List;

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
    protected long getTimestampFromLong(final JsonNode value) {
        final long milliSecondConversionFactor = 1_000;
        return super.getTimestampFromLong(value) * milliSecondConversionFactor;
    }
}
