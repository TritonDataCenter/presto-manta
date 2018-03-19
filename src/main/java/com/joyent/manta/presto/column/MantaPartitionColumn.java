/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;

/**
 * Class representing a single column within a logical table that is used solely
 * for partitioning data by filename.
 *
 * @since 1.0.0
 */
public class MantaPartitionColumn extends MantaColumn {
    private final int index;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param index index of the column relative to the regex matching groups
     * @param name name of column
     * @param type Presto type of column
     * @param comment comment about column
     * @param extraInfo additional information about column (eg JSON data type)
     * @param hidden flag indicating that column is hidden
     */
    public MantaPartitionColumn(final int index,
                                final String name,
                                final Type type,
                                final String comment,
                                final String extraInfo,
                                final boolean hidden) {
        super(name, type, comment, extraInfo, hidden);
        this.index = index;
    }

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param index index of the column relative to the regex matching groups
     * @param name name of column
     * @param typeString Presto type of column
     * @param comment comment about column
     * @param extraInfo additional information about column (eg JSON data type)
     * @param hidden flag indicating that column is hidden
     */
    @JsonCreator
    public MantaPartitionColumn(@JsonProperty("index") final int index,
                                @JsonProperty("name") final String name,
                                @JsonProperty("type") final String typeString,
                                @JsonProperty("comment") final String comment,
                                @JsonProperty("format") final String extraInfo,
                                @JsonProperty("hidden") final boolean hidden) {
        super(name, typeString, comment, extraInfo, hidden);
        this.index = index;
    }

    @JsonProperty
    public int getIndex() {
        return index;
    }

    @JsonProperty("format")
    @Override
    public String getExtraInfo() {
        return super.getExtraInfo();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        final MantaPartitionColumn that = (MantaPartitionColumn) o;

        return index == that.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("index", getIndex())
                .append("name", getName())
                .append("type", getType())
                .append("comment", getComment())
                .append("extraInfo", getExtraInfo())
                .append("hidden", isHidden())
                .toString();
    }
}
