/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.telegraf;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.presto.column.ColumnLister;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

/**
 * Hardcoded implementation of a {@link ColumnLister} that returns the fields
 * used in Telegraf data.
 *
 * @since 1.0.0
 */
public class MantaTelegrafColumnLister implements ColumnLister {
    /**
     * Presto map type defined for string keys and values.
     */
    public static final MapType STRING_MAP;

    static {
        final Class<? extends VarcharType> clazz = VarcharType.VARCHAR.getClass();
        final MethodHandles.Lookup lookup = MethodHandles.lookup();

        final Method keyBlockNativeEqualsMethod = MethodUtils.getMatchingMethod(
                clazz, "equalTo", Block.class, int.class, Block.class, int.class);
        Objects.requireNonNull(keyBlockNativeEqualsMethod,
                "Couldn't use reflection to get equalTo method");

        final Method keyNativeHashCodeMethod = MethodUtils.getMatchingMethod(
                clazz, "hash", Block.class, int.class);
        Objects.requireNonNull(keyNativeHashCodeMethod,
                "Couldn't use reflection to get hash method");

        final MethodType keyBlockHashCodeMt = MethodType.methodType(long.class, Block.class, int.class);

        final Method keyBlockHashCodeMethod = MethodUtils.getMatchingMethod(
                clazz, "hash", Block.class, int.class);
        Objects.requireNonNull(keyBlockHashCodeMethod,
                "Couldn't use reflection to get block hash method");

        final MethodHandle keyBlockNativeEqualsMh;
        final MethodHandle keyNativeHashCodeMh;
        final MethodHandle keyBlockHashCodeMh;

        try {
            keyBlockNativeEqualsMh = lookup.unreflect(keyBlockNativeEqualsMethod);
            keyNativeHashCodeMh = lookup.unreflect(keyNativeHashCodeMethod);
            keyBlockHashCodeMh = lookup.findStatic(MantaTelegrafColumnLister.class,
                    "varcharKeyHash", keyBlockHashCodeMt);
        } catch (IllegalAccessException e) {
            String msg = "Unable to access method handle";
            throw new MantaPrestoRuntimeException(msg, e);
        } catch (NoSuchMethodException e) {
            String msg = "No such method";
            throw new MantaPrestoRuntimeException(msg, e);
        }

        STRING_MAP = new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR,
                keyBlockNativeEqualsMh, keyNativeHashCodeMh, keyBlockHashCodeMh);
    }

    /**
     * Unmodifiable list of columns used in Telegraf data.
     */
    private static final List<MantaColumn> COLUMNS = ImmutableList.of(
            new MantaColumn("timestamp", TimestampType.TIMESTAMP, "Timestamp without TZ"),
            new MantaColumn("tags", STRING_MAP, "Associative array of tags"),
            new MantaColumn("name", VarcharType.VARCHAR, "Name of metric"),
            new MantaColumn("fields", STRING_MAP, "Associative array of metric fields")

    );

    /**
     * Creates a new instance.
     */
    public MantaTelegrafColumnLister() {
    }

    @Override
    public List<MantaColumn> listColumns(final MantaSchemaTableName tableName,
                                         final MantaLogicalTable table) {
        return COLUMNS;
    }

    /**
     * Calculates the hash for the key used by a varchar, varchar
     * map implementation.
     *
     * @param block block to calculate hash for
     * @param position block position
     * @return hash as long value
     */
    public static long varcharKeyHash(final Block block, final int position) {
        Objects.requireNonNull(block, "block is null");
        return VarcharType.VARCHAR.hash(block, position);
    }
}
