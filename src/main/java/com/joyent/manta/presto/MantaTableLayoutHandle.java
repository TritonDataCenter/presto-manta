/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joyent.manta.presto.tables.MantaSchemaTableName;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;

/**
 * {@link ConnectorTableLayoutHandle} implementation that represents a single
 * table by encoding a table name, schema and Manta logical table attributes.
 *
 * @since 1.0.0
 */
public class MantaTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final MantaSchemaTableName tableName;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param tableName table name definition object
     */
    @JsonCreator
    public MantaTableLayoutHandle(
            @JsonProperty("tableName") final MantaSchemaTableName tableName) {
        this.tableName = tableName;
    }

    @JsonProperty
    public MantaSchemaTableName getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MantaTableLayoutHandle that = (MantaTableLayoutHandle) o;

        return Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tableName", tableName)
                .toString();
    }
}
