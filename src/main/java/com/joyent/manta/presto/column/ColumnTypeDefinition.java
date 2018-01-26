/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.JsonType;
import com.google.common.collect.ImmutableMap;
import com.joyent.manta.presto.types.MapStringType;
import com.joyent.manta.presto.types.TimestampEpochSecondsType;

import java.util.Map;

/**
 * Enum defining the plain-text string type literal to Presto Type class mapping
 * used when manually configuring table's column definition.
 *
 * @since 1.0.0
 */
@SuppressWarnings("JavadocVariable")
public enum ColumnTypeDefinition {
    BIGINT("bigint", BigintType.BIGINT),
    BINARY("binary", VarbinaryType.VARBINARY),
    BOOLEAN("bool", BooleanType.BOOLEAN),
    DATE("date", DateType.DATE),
    DOUBLE("double", DoubleType.DOUBLE),
    INTEGER("int", IntegerType.INTEGER),
    JSON("json", JsonType.JSON),
    STRING("string", VarcharType.VARCHAR),
    STRING_STRING_MAP("string[string,string]", MapStringType.MAP_STRING_STRING),
    STRING_DOUBLE_MAP("string[string,double]", MapStringType.MAP_STRING_DOUBLE),
    TIMESTAMP_MILLISECONDS("timestamp-epoch-milliseconds", TimestampType.TIMESTAMP),
    TIMESTAMP_SECONDS("timestamp-epoch-seconds", TimestampEpochSecondsType.TIMESTAMP_EPOCH_SECONDS),;

    /**
     * Map providing a mapping between the plain-text column type identifier
     * and the class used by Presto to identify the type.
     */
    private static final Map<String, ColumnTypeDefinition> LOOKUP =
            buildLookup();

    private final String typeName;
    private final Type prestoType;

    /**
     * Creates a new instance with the specified mapping.
     *
     * @param typeName plain-text type identifier
     * @param prestoType presto class used by presto to identify the type
     */
    ColumnTypeDefinition(final String typeName, final Type prestoType) {
        this.typeName = typeName;
        this.prestoType = prestoType;
    }

    public String getTypeName() {
        return typeName;
    }

    public Type getPrestoType() {
        return prestoType;
    }

    /**
     * Finds the column type definition based on the input type as a string.
     *
     * @param type type to map to
     * @return enum representing the type or null if not found
     */
    public static ColumnTypeDefinition valueOfTypeName(final String type) {
        return LOOKUP.get(type);
    }

    private static Map<String, ColumnTypeDefinition> buildLookup() {
        ImmutableMap.Builder<String, ColumnTypeDefinition> builder =
                ImmutableMap.builder();

        for (ColumnTypeDefinition typeDefinition : ColumnTypeDefinition.values()) {
            addLookup(typeDefinition, builder);
        }

        return builder.build();
    }

    private static void addLookup(final ColumnTypeDefinition typeDefinition,
                                  final ImmutableMap.Builder<String, ColumnTypeDefinition> builder) {
        builder.put(typeDefinition.getTypeName(), typeDefinition);
    }
}
