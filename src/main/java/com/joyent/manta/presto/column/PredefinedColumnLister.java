/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.facebook.presto.spi.ConnectorSession;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Objects;

/**
 * {@link ColumnLister} implementation that simply returns the columns that
 * were assigned to a {@link MantaLogicalTable} object.
 *
 * @since 1.0.0
 */
public class PredefinedColumnLister implements ColumnLister {
    /**
     * Creates a new instance.
     */
    public PredefinedColumnLister() {
    }

    @Override
    public List<MantaColumn> listColumns(final MantaSchemaTableName tableName,
                                         final MantaLogicalTable table,
                                         final ConnectorSession session) {
        Objects.requireNonNull(table, "Table is null");
        Objects.requireNonNull(table.getColumns(),
                "Table column configuration is null");
        Validate.isTrue(table.getColumns().isPresent(),
                "Table column configuration must be specified in "
                        + "order to use predefined lister");

        return table.getColumns().get();
    }
}
