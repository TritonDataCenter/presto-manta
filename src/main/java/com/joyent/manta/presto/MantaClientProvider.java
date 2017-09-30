/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;

import javax.inject.Inject;
import javax.inject.Provider;

/**
 * Provides a configured instance of {@link MantaClient}. Remember, you will
 * need to close this instance.
 *
 * @since 1.0.0
 */
public class MantaClientProvider implements Provider<MantaClient> {
    private final ConfigContext config;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param config Manta configuration context
     */
    @Inject
    public MantaClientProvider(final ConfigContext config) {
        this.config = config;
    }

    @Override
    public MantaClient get() {
        return new MantaClient(config);
    }
}
