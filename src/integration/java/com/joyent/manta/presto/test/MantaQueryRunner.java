/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.test;

import com.facebook.presto.Session;
import com.facebook.presto.plugin.memory.MemoryPlugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.org.apache.commons.io.output.CloseShieldOutputStream;
import com.joyent.manta.presto.MantaCompressionType;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.MantaPlugin;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaLogicalTableProvider;
import io.airlift.tpch.TpchTable;
import org.apache.commons.compress.compressors.CompressorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.joyent.manta.client.MantaClient.SEPARATOR;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.lang.String.format;

public class MantaQueryRunner {
    public static final String CATALOG = "manta";

    private static final Logger LOG = LoggerFactory.getLogger(MantaQueryRunner.class);

    private final String schemaPath;

    public MantaQueryRunner(final String schemaPath) {
        this.schemaPath = schemaPath;
    }

    public DistributedQueryRunner createQueryRunner() {
        return createQueryRunner(Collections.emptyMap());
    }

    public DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties) {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema("default")
                .build();

        final DistributedQueryRunner queryRunner;

        try {
            queryRunner = new DistributedQueryRunner(session, 4, extraProperties);
        } catch (Exception e) {
            throw new AssertionError("Unable to create query runner", e);
        }

        try {
            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog("memory", "memory",
                    Collections.emptyMap());

            queryRunner.installPlugin(new MantaPlugin());
            queryRunner.createCatalog(CATALOG, "manta",
                    Collections.singletonMap("manta.schema.default", schemaPath));

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch",
                    Collections.emptyMap());

            return queryRunner;
        } catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static void createTestData(final String testPathPrefix,
                                      final MantaClient mantaClient,
                                      final DistributedQueryRunner queryRunner,
                                      final MantaDataFileType dataFileType,
                                      final MantaCompressionType compressionType)
            throws IOException, CompressorException {
        List<MantaLogicalTable> tablesDefinition = new LinkedList<>();
        Session session = testSessionBuilder().setCatalog("manta")
                .setSchema("default").build();

        final ObjectMapper mapper = new ObjectMapper();

        for (TpchTable<?> table : TpchTable.getTables()) {
            final String tableName = table.getTableName();
            final String rootPath = String.format(testPathPrefix + "%s", tableName);
            mantaClient.putDirectory(rootPath, true);

            final List<String> columns = queryColumnNames(queryRunner, session, tableName);

            final String sql = format("SELECT * FROM tpch.%s.%s",
                    TINY_SCHEMA_NAME, tableName);

            MaterializedResult result = queryRunner.execute(session, sql);

            String dataFilePath = rootPath + SEPARATOR + "data."
                    + dataFileType.getDefaultExtension();

            final OutputStream out;

            if (compressionType != null) {
                dataFilePath += "." + compressionType.getFileExtension();
                String compressor = compressionType.getCompressorName();
                out = MantaCompressionType.COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(compressor,
                        mantaClient.putAsOutputStream(dataFilePath));
            } else {
                out = mantaClient.putAsOutputStream(dataFilePath);
            }

            final char lineSeparator = "\n".charAt(0);
            int rows = 0;

            try {
                Iterator<MaterializedRow> itr = result.iterator();

                while (itr.hasNext()) {
                    MaterializedRow row = itr.next();
                    ObjectNode objectNode = new ObjectNode(JsonNodeFactory.instance);

                    for (int i = 0; i < row.getFieldCount(); i++) {
                        final Object field = row.getField(i);
                        final String column = columns.get(i);

                        writeJsonForType(objectNode, field, column);
                    }

                    // The mapper tries to close the stream, so we have to
                    // prevent it from doing so by using a close shield
                    CloseShieldOutputStream shieldOutputStream =
                            new CloseShieldOutputStream(out);
                    mapper.writeValue(shieldOutputStream, objectNode);
                    rows++;
                    if (itr.hasNext()) {
                        out.write(lineSeparator);
                    }
                }
            } finally {
                out.close();
                LOG.info("Wrote {} rows to table {} in file {}", rows, tableName,
                        dataFilePath);
            }

            tablesDefinition.add(new MantaLogicalTable(tableName,
                    rootPath, dataFileType));
        }

        final String tableDefinitionJsonPath = String.format(testPathPrefix + "%s",
                MantaLogicalTableProvider.TABLE_DEFINITION_FILENAME);

        try (OutputStream out = mantaClient.putAsOutputStream(tableDefinitionJsonPath)) {
            mapper.writeValue(out, tablesDefinition);
        }
    }

    private static List<String> queryColumnNames(final DistributedQueryRunner queryRunner,
                                                 final Session session,
                                                 final String tableName) {
        final String sql = format("SHOW COLUMNS FROM tpch.%s.%s",
                TINY_SCHEMA_NAME, tableName);

        MaterializedResult result = queryRunner.execute(session, sql);

        return result.getMaterializedRows().stream()
                .map(r -> Objects.toString(r.getField(0)))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private static void writeJsonForType(final ObjectNode node, Object field,
                                         final String column) {
        if (field.getClass().equals(int.class)) {
            node.put(column, (int)field);
        } else if (field.getClass().equals(Integer.class)) {
            node.put(column, (Integer)field);
        } else if (field.getClass().equals(long.class)) {
            node.put(column, (long)field);
        } else if (field.getClass().equals(Long.class)) {
            node.put(column, (Long)field);
        } else if (field.getClass().equals(short.class)) {
            node.put(column, (short)field);
        } else if (field.getClass().equals(short.class)) {
            node.put(column, (Short)field);
        } else if (field.getClass().equals(float.class)) {
            node.put(column, (float)field);
        } else if (field.getClass().equals(Float.class)) {
            node.put(column, (Float)field);
        } else if (field.getClass().equals(double.class)) {
            node.put(column, (double)field);
        } else if (field.getClass().equals(Double.class)) {
            node.put(column, (Double)field);
        } else if (field.getClass().equals(boolean.class)) {
            node.put(column, (boolean)field);
        } else if (field.getClass().equals(Boolean.class)) {
            node.put(column, (Boolean)field);
        } else if (field.getClass().equals(BigDecimal.class)) {
            node.put(column, (BigDecimal)field);
        } else if (field.getClass().equals(String.class)) {
            node.put(column, (String)field);
        }
    }
}
