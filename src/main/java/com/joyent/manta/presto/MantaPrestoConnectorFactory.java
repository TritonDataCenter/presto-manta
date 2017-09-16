/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import com.joyent.manta.config.MapConfigContext;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Class the provides Connector instances configured for use with Manta.
 */
public class MantaPrestoConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "manta";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new MantaPrestoHandleResolver();
    }

    static Injector buildInjector(final String connectorId,
                           final Map<String, String> config,
                           final ConnectorContext context) throws Exception {
        requireNonNull(config, "Manta Connector configuration must not be null");

        // A plugin is not required to use Guice; it is just very convenient
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new MantaPrestoModule(connectorId, context.getTypeManager(), config));

        return app
                .strictConfig()
                .doNotInitializeLogging()
                .initialize();
    }

    @Override
    public Connector create(final String connectorId,
                            final Map<String, String> config,
                            final ConnectorContext context) {
        try {
            Injector injector = buildInjector(connectorId, config, context);
            return injector.getInstance(MantaPrestoConnector.class);
        }
        catch (Exception e) {
            final String msg = "Error creating new connector";
            MantaPrestoRuntimeException re =
                    new MantaPrestoRuntimeException(msg, e);

            re.setContextValue("connectorId", connectorId);

            final String configDump = new MapConfigContext(config).toString();
            re.setContextValue("config", configDump);

            final String contextDump = ToStringBuilder.reflectionToString(context);
            re.setContextValue("connectorContext", contextDump);

            throw re;
        }
    }
}
