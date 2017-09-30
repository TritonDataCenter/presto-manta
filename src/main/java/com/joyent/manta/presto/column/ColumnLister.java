/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;

import java.util.List;

/**
 * Interface representing an object that provides operations for listing all
 * of the columns for a logical table in Manta.
 *
 * @since 1.0.0
 */
public interface ColumnLister {
    /**
     * Provides an ordered list of columns for a given table.
     *
     * @param tableName table name object with schema specified
     * @param table logical table definition
     *
     * @return list of columns for table
     */
    List<MantaColumn> listColumns(MantaSchemaTableName tableName,
                                  MantaLogicalTable table);
}
