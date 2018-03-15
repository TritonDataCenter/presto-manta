/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import com.joyent.manta.presto.types.TimestampEpochSecondsType;

import java.util.Collections;
import java.util.Set;

/**
 * Plugin definition class for the Manta Presto Connector.
 *
 * @since 1.0.0
 */
public class MantaPlugin implements Plugin {
    private static Set<Type> mantaTypes =
            ImmutableSet.of(TimestampEpochSecondsType.TIMESTAMP_EPOCH_SECONDS);

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return Collections.singletonList(new MantaConnectorFactory());
    }

    @Override
    public Iterable<Type> getTypes() {
        return mantaTypes;
    }
}
