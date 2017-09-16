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
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.MapConfigContext;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.Map;

import static java.util.Objects.requireNonNull;

import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

/**
 *
 */
public class MantaPrestoModule implements Module {
    /**
     * Presto defaults to use JDK logging. We use it explicitly here so that
     * we can be certain to output the configuration before dependency injection
     * is started. */
    private static final Logger log = Logger.getLogger(MantaPrestoModule.class.getName());

    private final String connectorId;
    private final TypeManager typeManager;
    private final ConfigContext config;

    public MantaPrestoModule(final String connectorId,
                             final TypeManager typeManager,
                             final Map<String, String> configParams) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        requireNonNull(configParams, "Configuration is null");
        this.config = buildConfigContext(configParams);

        if (log.isLoggable(Level.FINE)) {
            String msg = String.format("Configuration loaded:\n%s", this.config);
            log.fine(msg);
        }
    }

    private ConfigContext buildConfigContext(final Map<String, String> configParams) {
        return new ChainedConfigContext(
                new EnvVarConfigContext(),
                new MapConfigContext(System.getProperties()),
                new MapConfigContext(configParams),
                new DefaultsConfigContext()
        );
    }

    @Override
    public void configure(final Binder binder) {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(ConfigContext.class).toInstance(this.config);
        binder.bind(MantaClient.class).toProvider(MantaClientProvider.class).asEagerSingleton();
        binder.bind(MantaPrestoConnector.class).in(Scopes.SINGLETON);
        binder.bind(MantaPrestoConnectorId.class).toInstance(new MantaPrestoConnectorId(connectorId));
        binder.bind(MantaPrestoMetadata.class).in(Scopes.SINGLETON);
        binder.bind(MantaPrestoClient.class).in(Scopes.SINGLETON);
        binder.bind(MantaPrestoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MantaPrestoRecordSetProvider.class).in(Scopes.SINGLETON);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(MantaPrestoTypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(MantaPrestoTable.class));
    }
}
