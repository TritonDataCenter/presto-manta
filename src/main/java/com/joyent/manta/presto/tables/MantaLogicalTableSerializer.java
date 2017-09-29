/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 *
 */
public class MantaLogicalTableSerializer extends JsonSerializer<MantaLogicalTable> {
    public MantaLogicalTableSerializer() {
    }

    @Override
    public void serialize(final MantaLogicalTable value,
                          final JsonGenerator gen,
                          final SerializerProvider serializers) throws IOException {
        gen.writeStartObject();

        gen.writeStringField("name", value.getTableName());
        gen.writeStringField("rootPath", value.getRootPath());
        gen.writeStringField("dataFileType", value.getDataFileType().toString());

        if (value.getDirectoryFilterRegex() != null) {
            gen.writeStringField("directoryFilterRegex",
                    value.getDirectoryFilterRegex().toString());
        }

        if (value.getFilterRegex() != null) {
            gen.writeStringField("filterRegex",
                    value.getFilterRegex().toString());
        }

        gen.writeEndObject();
    }
}
