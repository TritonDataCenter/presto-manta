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
import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
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
 * Class that provides instances of {@link MantaLogicalTable} based on reading
 * the <code>presto-tables.json</code> file associated with a schema.
 *
 * @since 1.0.0
 */
public class MantaLogicalTableProvider {
    /**
     * Filename for table definition file per schema. This file should be located
     * within the schema directory.
     */
    public static final String TABLE_DEFINITION_FILENAME = "presto-tables.json";

    /**
     * Cache of the table definition file object ({@link MantaLogicalTable})
     * per schema.
     */
    private final Cache<String, Map<String, MantaLogicalTable>> tableListCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(1, TimeUnit.MINUTES).build();

    /**
     * Mapping of schema name to directory path as configured within the Presto
     * catalog configuration file.
     */
    private final Map<String, String> schemaMapping;

    /**
     * Jackson JSON serializer / deserializer.
     */
    private final ObjectMapper objectMapper;

    /**
     * Manta client instance that allows for direct operations against Manta.
     */
    private final MantaClient mantaClient;

    /**
     * {@link Callable} implementation that allows for the asynchronous reading
     * and parsing of a <code>presto-tables.json</code> file.
     */
    private final class TableDefinitionLoader implements Callable<Map<String, MantaLogicalTable>> {
        private final String pathToDefinition;

        TableDefinitionLoader(final String pathToDefinition) {
            this.pathToDefinition = pathToDefinition;
        }

        @Override
        public Map<String, MantaLogicalTable> call() throws Exception {
            try (MantaObjectInputStream in = mantaClient.getAsInputStream(pathToDefinition)) {
                TypeReference type = new TypeReference<Set<MantaLogicalTable>>() { };
                Set<MantaLogicalTable> tables = objectMapper.readValue(in, type);

                return tables.stream().collect(Collectors.toMap(MantaLogicalTable::getTableName, item -> item));
            }
        }
    }

    /**
     * Creates a new instance based on the passed parameters.
     *
     * @param schemaMapping mapping of schema name to directory path
     * @param objectMapper Jackson JSON serializer / deserializer.
     * @param mantaClient manta client instance
     */
    @Inject
    public MantaLogicalTableProvider(@Named("SchemaMapping") final Map<String, String> schemaMapping,
                                     final ObjectMapper objectMapper,
                                     final MantaClient mantaClient) {
        this.schemaMapping = schemaMapping;
        this.objectMapper = objectMapper;
        this.mantaClient = mantaClient;
    }

    /**
     * Get a table based on schema and name.
     *
     * @param schemaName schema as defined in Presto catalog configuration
     * @param tableName table name as defined in the tables definition file
     * @return object representing a logical table
     *
     * @throws MantaPrestoTableNotFoundException when the table can't be found
     */
    public MantaLogicalTable getTable(final String schemaName, final String tableName) {
        MantaLogicalTable table = tablesForSchema(schemaName).get(tableName);

        if (table == null) {
            SchemaTableName mstn = new SchemaTableName(schemaName, tableName);
            throw new MantaPrestoTableNotFoundException(mstn);
        }

        return table;
    }

    /**
     * Gets a {@link Map} with keys of the table name and values of the
     * actual {@link MantaLogicalTable} instances.
     *
     * @param schemaName schema as defined in Presto catalog configuration
     * @return map of table names to tables
     *
     * @throws MantaPrestoSchemaNotFoundException when no mapping can be found for schema
     */
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

    /**
     * Gets a sorted list of all {@link MantaSchemaTableName} instances
     * associated with the specified schema.
     *
     * @param schemaName schema as defined in Presto catalog configuration
     * @return sorted list (by name) of all tables for a schema
     *
     * @throws MantaPrestoSchemaNotFoundException when no mapping can be found for schema
     */
    public List<SchemaTableName> tableListForSchema(final String schemaName) {
        return tablesForSchema(schemaName)
                .values()
                .stream()
                .sorted()
                .map(t -> new MantaSchemaTableName(schemaName, t))
                .collect(Collectors.toList());
    }

    /**
     * Creates the Manta directory path to the tables definition file based
     * on a schema's directory mapping.
     */
    private String pathToTableDefinition(final String schemaName) {
        final String dir = schemaMapping.get(schemaName);

        if (dir == null) {
            MantaPrestoSchemaNotFoundException e = new MantaPrestoSchemaNotFoundException(schemaName);
            e.setContextValue("schemaMapping", Joiner.on(',').withKeyValueSeparator('=').join(schemaMapping));

            throw e;
        }

        return MantaUtils.formatPath(dir + SEPARATOR
                + TABLE_DEFINITION_FILENAME);
    }
}
