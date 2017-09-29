/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.presto.MantaSchemaTableName;
import com.joyent.manta.presto.exceptions.MantaPrestoSchemaNotFoundException;
import com.joyent.manta.presto.exceptions.MantaPrestoTableNotFoundException;
import com.joyent.manta.util.MantaUtils;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 *
 */
public class MantaLogicalTableProvider {
    public static final String TABLE_DEFINITION_FILENAME = "presto-tables.json";

    private final Cache<String, Map<String, MantaLogicalTable>> tableListCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(1, TimeUnit.MINUTES).build();
    private final Map<String, String> schemaMapping;
    private final ObjectMapper objectMapper;
    private final MantaClient mantaClient;

    private final class TableDefinitionLoader implements Callable<Map<String, MantaLogicalTable>> {
        private final String pathToDefinition;

        public TableDefinitionLoader(final String pathToDefinition) {
            this.pathToDefinition = pathToDefinition;
        }

        @Override
        public Map<String, MantaLogicalTable> call() throws Exception {
            try (MantaObjectInputStream in = mantaClient.getAsInputStream(pathToDefinition)) {
                TypeReference type = new TypeReference<Set<MantaLogicalTable>>() {};
                Set<MantaLogicalTable> tables = objectMapper.readValue(in, type);

                return tables.stream().collect(Collectors.toMap(MantaLogicalTable::getTableName, item -> item));
            }
        }
    }

    @Inject
    public MantaLogicalTableProvider(@Named("SchemaMapping") final Map<String, String> schemaMapping,
                                     final ObjectMapper objectMapper,
                                     final MantaClient mantaClient) {
        this.schemaMapping = schemaMapping;
        this.objectMapper = objectMapper;
        this.mantaClient = mantaClient;
    }

    public MantaLogicalTable getTable(final String schemaName, final String tableName) {
        MantaLogicalTable table = tablesForSchema(schemaName).get(tableName);

        if (table == null) {
            MantaSchemaTableName mstn = new MantaSchemaTableName(
                    schemaName, table);
            throw new MantaPrestoTableNotFoundException(mstn);
        }

        return table;
    }

    public Map<String, MantaLogicalTable> tablesForSchema(final String schemaName) {
        final String pathToDefinition = pathToTableDefinition(schemaName);
        final TableDefinitionLoader loader = new TableDefinitionLoader(pathToDefinition);

        try {
            return tableListCache.get(schemaName, loader);
        } catch (ExecutionException e) {
            String msg = "Error loading table definition for schema";

            MantaPrestoSchemaNotFoundException me = new MantaPrestoSchemaNotFoundException(
                    schemaName, msg, e.getCause());
            me.setContextValue("schemaName", schemaName);
            me.setContextValue("pathToDefinition", pathToDefinition);
            throw me;
        }
    }

    public List<SchemaTableName> tableListForSchema(final String schemaName) {
        return tablesForSchema(schemaName)
                .values()
                .stream()
                .sorted()
                .map(t -> new SchemaTableName(schemaName, t.getTableName()))
                .collect(Collectors.toList());
    }

    private String pathToTableDefinition(final String schemaName) {
        final String dir = schemaMapping.get(schemaName);

        if (dir == null) {
            throw new MantaPrestoSchemaNotFoundException(schemaName);
        }

        return MantaUtils.formatPath(dir + SEPARATOR
                + TABLE_DEFINITION_FILENAME);
    }
}
