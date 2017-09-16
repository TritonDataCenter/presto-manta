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
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

/**
 * Guice module that loads in the configuration needed to inject the
 * dependencies for the Manta Presto Connector.
 */
public class MantaPrestoModule implements Module {
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

        LoggerFactory.getLogger(getClass()).debug("Manta Configuration: {}", this.config);
    }

    private ConfigContext buildConfigContext(final Map<String, String> configParams) {
        if (configParams != null && !configParams.isEmpty()) {
            return new ChainedConfigContext(
                    new EnvVarConfigContext(),
                    new MapConfigContext(System.getProperties()),
                    new MapConfigContext(configParams),
                    new DefaultsConfigContext());
        } else {
            return new ChainedConfigContext(
                    new EnvVarConfigContext(),
                    new MapConfigContext(System.getProperties()),
                    new DefaultsConfigContext());
        }
    }

    @Override
    public void configure(final Binder binder) {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(ConfigContext.class).toInstance(this.config);
        binder.bind(MantaClient.class).toProvider(MantaClientProvider.class).in(Scopes.SINGLETON);
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
