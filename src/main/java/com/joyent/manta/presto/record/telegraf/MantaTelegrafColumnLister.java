/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.telegraf;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.presto.column.ColumnLister;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;

import java.util.List;

import static com.joyent.manta.presto.types.MapStringType.MAP_STRING_DOUBLE;
import static com.joyent.manta.presto.types.MapStringType.MAP_STRING_STRING;
import static com.joyent.manta.presto.types.TimestampEpochSecondsType.TIMESTAMP_EPOCH_SECONDS;

/**
 * Hardcoded implementation of a {@link ColumnLister} that returns the fields
 * used in Telegraf data.
 *
 * @since 1.0.0
 */
public class MantaTelegrafColumnLister implements ColumnLister {
    /**
     * Unmodifiable list of columns used in Telegraf data.
     */
    private static final List<MantaColumn> COLUMNS = ImmutableList.of(
            new MantaColumn("timestamp", TIMESTAMP_EPOCH_SECONDS, "Timestamp without TZ"),
            new MantaColumn("tags", MAP_STRING_STRING, "Associative array of tags"),
            new MantaColumn("name", VarcharType.VARCHAR, "Name of metric"),
            new MantaColumn("fields", MAP_STRING_DOUBLE, "Associative array of metric fields")
    );

    /**
     * Creates a new instance.
     */
    public MantaTelegrafColumnLister() {
    }

    @Override
    public List<MantaColumn> listColumns(final MantaSchemaTableName tableName,
                                         final MantaLogicalTable table,
                                         final ConnectorSession session) {
        return COLUMNS;
    }
}
