/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joyent.manta.presto.types.TypeUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Class representing a single column within a logical table.
 *
 * @since 1.0.0
 */
public class MantaColumn extends ColumnMetadata implements ColumnHandle {
    /**
     * Creates a new instance based on the specified parameters.
     *  @param name name of column
     * @param type Presto type of column
     * @param comment comment about column
     * @param extraInfo additional information about column (eg JSON data type)
     * @param hidden flag indicating that column is hidden
     */
    public MantaColumn(final String name,
                       final Type type,
                       final String comment,
                       final String extraInfo,
                       final boolean hidden) {
        super(name, type, comment, extraInfo, hidden);
    }

    /**
     * Creates a new instance based on the specified parameters.
     *  @param name name of column
     * @param typeString Presto type of column
     * @param comment comment about column
     * @param extraInfo additional information about column (eg JSON data type)
     * @param hidden flag indicating that column is hidden
     */
    @JsonCreator
    public MantaColumn(@JsonProperty("name") final String name,
                       @JsonProperty("type") final String typeString,
                       @JsonProperty("comment") final String comment,
                       @JsonProperty("format") final String extraInfo,
                       @JsonProperty("hidden") final boolean hidden) {
        super(name, TypeUtils.parseAndValidateTypeFromString(typeString),
                comment, extraInfo, hidden);
    }

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param name name of column
     * @param type Presto type of column
     * @param comment comment about column
     */
    public MantaColumn(final String name,
                       final Type type,
                       final String comment) {
        this(name, type, comment, null, false);
    }

    @JsonProperty
    @Override
    public String getName() {
        return super.getName();
    }

    @Override
    public Type getType() {
        return super.getType();
    }

    @JsonProperty("type")
    public String getTypeAsString() {
        return super.getType().toString();
    }

    @JsonProperty
    @Override
    public String getComment() {
        return super.getComment();
    }

    @JsonProperty("format")
    @Override
    public String getExtraInfo() {
        return super.getExtraInfo();
    }

    @JsonProperty
    @Override
    public boolean isHidden() {
        return super.isHidden();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", super.getName())
                .append("type", super.getType())
                .append("comment", super.getComment())
                .append("extraInfo", super.getExtraInfo())
                .append("hidden", super.isHidden())
                .toString();
    }
}
