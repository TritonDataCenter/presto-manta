/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * {@link Callable} implementation that allows for the asynchronous reading
 * and parsing of a <code>presto-tables.json</code> file.
 */
abstract class TableDefinitionLoader implements Callable<Map<String, MantaLogicalTable>> {
    private final ObjectMapper objectMapper;

    /**
     * Creates a new instance of a loader based on the specified URL.
     *
     * @param objectMapper Jackson JSON serializer / deserializer.
     */
    TableDefinitionLoader(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Map<String, MantaLogicalTable> call() throws Exception {
        try (InputStream in = openStream()) {
            TypeReference type = new TypeReference<Set<MantaLogicalTable>>() { };
            Set<MantaLogicalTable> tables = objectMapper
                    .enable(JsonParser.Feature.ALLOW_COMMENTS)
                    .enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES)
                    .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                    .enable(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)
                    .readValue(in, type);

            ImmutableMap.Builder<String, MantaLogicalTable> builder =
                    new ImmutableMap.Builder<>();

            for (MantaLogicalTable table : tables) {
                final String key = Validate.notBlank(table.getTableName());
                final MantaLogicalTable val = table;
                builder.put(key, val);
            }

            try {
                return builder.build();
            } catch (IllegalArgumentException e) {
                String msg = "Multiple tables specified with the same name. "
                        + "Please check configuration for the schema.";
                MantaPrestoIllegalArgumentException me = new MantaPrestoIllegalArgumentException(msg, e);

                if (in instanceof MantaObjectInputStream) {
                    MantaObjectInputStream min = (MantaObjectInputStream)in;
                    me.setContextValue("tableDefinitionPath", min.getPath());
                } else {
                    me.setContextValue("inputStream", in);
                }

                throw me;
            }

        }
    }

    /**
     * Opens a stream to the table definition JSON file.
     *
     * @return stream instance containing JSON data
     * @throws IOException thrown when stream can't be opened
     */
    abstract InputStream openStream() throws IOException;
}
