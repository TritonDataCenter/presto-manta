/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.types;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Objects;

/**
 * Presto type definition class that defines a type as a
 * Map with String keys and values.
 *
 * @since 1.0.0
 */
public final class MapStringType {
    /**
     * Presto map type defined for string keys and values.
     */
    public static final MapType MAP_STRING_STRING = buildMapStringStringType();

    /**
     * Presto map type defined for string keys and {@link java.lang.Double} values.
     */
    public static final MapType MAP_STRING_DOUBLE = buildMapStringDoubleType();

    /**
     * Private constructor because there is no need for non-static instances.
     */
    private MapStringType() {
    }

    private static MapType buildMapStringStringType() {
        return buildMapWithStringKeys(VarcharType.VARCHAR);
    }

    private static MapType buildMapStringDoubleType() {
        return buildMapWithStringKeys(DoubleType.DOUBLE);
    }

    private static MapType buildMapWithStringKeys(final Type valueType) {
        final MethodHandles.Lookup lookup = MethodHandles.lookup();

        final MethodType keyBlockNativeEqualsMt = MethodType.methodType(boolean.class, Block.class,
                int.class, Block.class, int.class);
        final MethodType keyNativeHashCodeMt = MethodType.methodType(long.class, Block.class, int.class);
        final MethodType keyBlockHashCodeMt = MethodType.methodType(long.class, Block.class, int.class);

        final MethodHandle keyBlockNativeEqualsMh;
        final MethodHandle keyNativeHashCodeMh;
        final MethodHandle keyBlockHashCodeMh;

        try {
            keyBlockNativeEqualsMh = lookup.findVirtual(VarcharType.class, "equalTo", keyBlockNativeEqualsMt);
            keyNativeHashCodeMh = lookup.findVirtual(VarcharType.class, "hash", keyNativeHashCodeMt);
            keyBlockHashCodeMh = lookup.findStatic(MapStringType.class,
                    "varcharKeyHash", keyBlockHashCodeMt);
        } catch (IllegalAccessException e) {
            String msg = "Unable to access method handle";
            throw new MantaPrestoRuntimeException(msg, e);
        } catch (NoSuchMethodException e) {
            String msg = "No such method";
            throw new MantaPrestoRuntimeException(msg, e);
        }

        return new MapType(VarcharType.VARCHAR, valueType,
                keyBlockNativeEqualsMh, keyNativeHashCodeMh, keyBlockHashCodeMh);
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
