/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaPrestoTypeDeserializer extends FromStringDeserializer<Type> {
    private final TypeManager typeManager;

    @Inject
    public MantaPrestoTypeDeserializer(final TypeManager typeManager) {
        super(Type.class);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected Type _deserialize(final String value, final DeserializationContext context) {
        Type type = typeManager.getType(parseTypeSignature(value));
        checkArgument(type != null, "Unknown type %s", value);
        return type;
    }
}
