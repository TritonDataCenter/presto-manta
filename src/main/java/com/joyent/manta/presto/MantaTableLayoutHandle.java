/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joyent.manta.presto.tables.MantaSchemaTableName;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * {@link ConnectorTableLayoutHandle} implementation that represents a single
 * table by encoding a table name, schema and Manta logical table attributes.
 *
 * @since 1.0.0
 */
public class MantaTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final MantaSchemaTableName tableName;
    private final TupleDomain<ColumnHandle> predicate;
    private final Optional<Set<ColumnHandle>> desiredColumns;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param tableName table name definition object
     */
    public MantaTableLayoutHandle(final MantaSchemaTableName tableName) {
        this(tableName, TupleDomain.all(), Optional.empty());
    }

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param tableName table name definition object
     * @param predicate predicate indication the partitioning mode
     * @param desiredColumns an optional set of the columns desired in the query
     */
    @JsonCreator
    public MantaTableLayoutHandle(@JsonProperty("tableName") final MantaSchemaTableName tableName,
                                  @JsonProperty("predicate") final TupleDomain<ColumnHandle> predicate,
                                  @JsonProperty("desiredColumns") final Optional<Set<ColumnHandle>> desiredColumns) {
        this.tableName = tableName;
        this.predicate = predicate;
        this.desiredColumns = desiredColumns;
    }

    @JsonProperty
    public MantaSchemaTableName getTableName() {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPredicate() {
        return predicate;
    }

    @JsonProperty
    public Optional<Set<ColumnHandle>> getDesiredColumns() {
        return desiredColumns;
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

        return Objects.equals(tableName, that.tableName)
                && Objects.equals(predicate, that.predicate)
                && Objects.equals(desiredColumns, that.desiredColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tableName", tableName)
                .append("constraint", predicate)
                .append("desiredColumns", desiredColumns)
                .toString();
    }
}
