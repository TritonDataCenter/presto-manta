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
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

/**
 *
 */
public class MantaPrestoModule implements Module {
    private final String connectorId;
    private final TypeManager typeManager;

    public MantaPrestoModule(final String connectorId, final TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(final Binder binder) {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(MantaPrestoConnector.class).in(Scopes.SINGLETON);
        binder.bind(MantaPrestoConnectorId.class).toInstance(new MantaPrestoConnectorId(connectorId));
        binder.bind(MantaPrestoMetadata.class).in(Scopes.SINGLETON);
        binder.bind(MantaPrestoClient.class).in(Scopes.SINGLETON);
        binder.bind(MantaPrestoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MantaPrestoRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MantaPrestoConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(MantaPrestoTypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(MantaPrestoTable.class));
    }
}
