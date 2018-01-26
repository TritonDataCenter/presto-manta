/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.telegraf;

import com.facebook.presto.spi.RecordCursor;
import com.fasterxml.jackson.databind.ObjectReader;
import com.joyent.manta.presto.MantaCountingInputStream;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.record.json.MantaJsonRecordCursor;

import java.util.List;
import java.util.function.Supplier;

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
     * @param streamRecreator function used to recreate the underlying stream providing the JSON data
     * @param columns list of columns in table
     * @param objectPath path to object in Manta
     * @param totalBytes total number of bytes in source object
     * @param countingStream input stream that counts the number of bytes processed
     * @param streamingReader streaming json deserialization reader
     */
    public MantaTelegrafJsonRecordCursor(final Supplier<MantaCountingInputStream> streamRecreator,
                                         final List<MantaColumn> columns,
                                         final String objectPath,
                                         final Long totalBytes,
                                         final MantaCountingInputStream countingStream,
                                         final ObjectReader streamingReader) {
        super(streamRecreator, columns, objectPath, totalBytes, countingStream, streamingReader);
    }
}
