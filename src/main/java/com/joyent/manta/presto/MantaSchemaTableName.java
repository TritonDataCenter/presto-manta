/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joyent.manta.presto.tables.MantaLogicalTable;

import java.util.Objects;

/**
 * A Manta Presto specific implementation of {@link SchemaTableName} that
 * preserves the directory path structure with case sensitivity.
 */
public class MantaSchemaTableName extends SchemaTableName
        implements ConnectorTableHandle {
    private final MantaLogicalTable table;

    @JsonCreator
    public MantaSchemaTableName(@JsonProperty("schemaName") final String schemaName,
                                @JsonProperty("table") final MantaLogicalTable table) {
        super(schemaName, table.getTableName());
        this.table = table;
    }

    @Override
    public SchemaTablePrefix toSchemaTablePrefix() {
        return new SchemaTablePrefix(getSchemaName(), getTableName());
    }

    @JsonProperty("table")
    public MantaLogicalTable getTable() {
        return table;
    }

    @JsonProperty("schemaName")
    @Override
    public String getSchemaName() {
        return super.getSchemaName();
    }

    @JsonProperty("tableName")
    @Override
    public String getTableName() {
        return super.getTableName();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) return false;

        final MantaSchemaTableName that = (MantaSchemaTableName) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), table);
    }
}
