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

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Class the provides Connector instances configured for use with Manta.
 */
public class MantaConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "manta";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new MantaHandleResolver();
    }

    @Override
    public Connector create(final String connectorId,
                            final Map<String, String> config,
                            final ConnectorContext context) {
        requireNonNull(config, "Manta Connector configuration must not be null");

        return null;
    }
}
