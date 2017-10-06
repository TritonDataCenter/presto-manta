/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.json;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Provider;

/**
 * {@link Provider} implementation that provides {@link ObjectMapper}
 * implementations configured for parsing streaming JSON data files. We have a
 * separate instance of {@link ObjectMapper} just for processing JSON data files
 * because the {@link ObjectMapper} connector is heavily customized and that
 * customizations slows down the parsing of the incoming data streams.
 *
 * @since 1.0.0
 */
public class MantaJsonDataFileObjectMapperProvider implements Provider<ObjectMapper> {
    @Override
    public ObjectMapper get() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.enable(DeserializationFeature.USE_LONG_FOR_INTS);
        mapper.enable(DeserializationFeature.EAGER_DESERIALIZER_FETCH);

        return mapper;
    }
}
