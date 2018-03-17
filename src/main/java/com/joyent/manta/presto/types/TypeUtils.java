/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.types;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import org.apache.commons.lang3.Validate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * <p>Utility class providing common methods for working with Presto {@link Type}
 * instances.</p>
 *
 * <p>Note: This class could use some refactoring because it may be possible
 * to do everything that is done in this class natively within Presto's type
 * system.</p>
 *
 * @since 1.0.0
 */
public final class TypeUtils {
    /**
     * Provider for type registry used for resolving Presto native types.
     */
    @Inject
    private static TypeManager typeRegistry;

    private TypeUtils() {
    }

    /**
     * Parses a given type name as a string and converts it to a Presto {@link Type}
     * object.
     *
     * @param typeNameAsString type name to parse
     * @return Presto type object relating to human readable type name or null if not found
     */
    @Nullable
    public static Type parseTypeFromString(final String typeNameAsString) {
        Validate.notBlank(typeNameAsString, "type name must not be blank");

        // Attempt to parse type name using Presto's native type names
        final Type nativeType = parsePrestoNativeTypeFromString(typeNameAsString);

        if (nativeType != null) {
            return nativeType;
        }

        return parsePrestoMantaTypeFromString(typeNameAsString);
    }

    /**
     * Parses a given type name as a string and converts it to a Presto {@link Type}
     * object. This method will fail with {@link MantaPrestoIllegalArgumentException}
     * if the type can't be mapped.
     *
     * @param typeNameAsString type name to parse
     * @return Presto type object relating to human readable type name or null if not found
     */
    @Nonnull
    public static Type parseAndValidateTypeFromString(final String typeNameAsString) {
        final Type type = parseTypeFromString(typeNameAsString);

        if (type == null) {
            String msg = "Invalid string specified as Presto type object";
            MantaPrestoIllegalArgumentException e = new MantaPrestoIllegalArgumentException(msg);
            e.setContextValue("typeNameAsString", typeNameAsString);
            throw e;
        }

        return type;
    }

    /**
     * Parses a given type name as a string and converts it to a Presto {@link Type}
     * object for type names specific to the Presto Manta plugin.
     *
     * @param typeNameAsString type name to parse
     * @return Presto type object relating to human readable type name or null if not found
     */
    @Nullable
    public static Type parsePrestoMantaTypeFromString(final String typeNameAsString) {
        final Type type;

        switch (typeNameAsString) {
            // Alias for varchar
            case "string":
                type = VarcharType.VARCHAR;
                break;
            case "map(varchar,varchar)":
                type = MapStringType.MAP_STRING_STRING;
                break;
            case "map(varchar,double)":
                type = MapStringType.MAP_STRING_DOUBLE;
                break;
            default:
                type = null;
                break;
        }

        return type;
    }

    /**
     * Parses a given type name as a string and converts it to a Presto {@link Type}
     * object for type names specific to the Presto.
     *
     * @param typeNameAsString type name to parse
     * @return Presto type object relating to human readable type name or null if not found
     */
    @Nullable
    public static Type parsePrestoNativeTypeFromString(final String typeNameAsString) {
        final TypeSignature typeSignature = TypeSignature.parseTypeSignature(typeNameAsString);

        if (typeSignature == null) {
            return null;
        }

        return typeRegistry.getType(typeSignature);
    }
}
