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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.MantaPlugin;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.compression.MantaCompressionType;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaLogicalTableProvider;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchTable;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
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

    private final static CompressorStreamFactory COMPRESSOR_STREAM_FACTORY =
            new CompressorStreamFactory();

    private final static MinimalPrettyPrinter PRETTY_PRINTER =
            new MinimalPrettyPrinter("\n");

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

            final Path tempFile = createTempFile(tableName, compressionType);

            try (OutputStream out = tempFileOutputStream(tempFile, compressionType)) {
                MaterializedRowToObjectNodeIterator itr = new MaterializedRowToObjectNodeIterator(
                        columns, result.iterator());
                mapper.writerFor(ObjectNode.class).with(PRETTY_PRINTER)
                        .writeValues(out).writeAll(itr);
            }

            final String dataFilePath = createDataFilePath(rootPath, dataFileType,
                    compressionType);

            mantaClient.put(dataFilePath, tempFile.toFile());
            LOG.info("Pushing table [{}] temp file to Manta path [{}]", tableName,
                    dataFilePath);

            final List<Type> types = result.getTypes();

            final ImmutableList.Builder<MantaColumn> mantaColumns = new ImmutableList.Builder<>();

            int index = 0;
            for (TpchColumn<?> c : table.getColumns()) {
                final Type type = types.get(index++);
                mantaColumns.add(new MantaColumn(c.getSimplifiedColumnName(), type, null));
            }

            tablesDefinition.add(new MantaLogicalTable(tableName,
                    rootPath, dataFileType, null, mantaColumns.build()));
        }

        final String tableDefinitionJsonPath = String.format(testPathPrefix + "%s",
                MantaLogicalTableProvider.TABLE_DEFINITION_FILENAME);

        try (OutputStream out = mantaClient.putAsOutputStream(tableDefinitionJsonPath)) {
            mapper.writeValue(out, tablesDefinition);
        }
    }

    private static String createDataFilePath(final String rootPath,
                                             final MantaDataFileType dataFileType,
                                             final MantaCompressionType compressionType) {
        String dataFilePath = rootPath + SEPARATOR + "data."
                + dataFileType.getDefaultExtension();

        if (compressionType != null) {
            dataFilePath += "." + compressionType.getFileExtension();
        }

        return dataFilePath;
    }

    private static Path createTempFile(final String tableName,
                                       final MantaCompressionType compressionType)
            throws IOException {
        String suffix = ".ndjson";

        if (compressionType != null) {
            suffix += "." + compressionType.getFileExtension();
        }

        Path tempFile = Files.createTempFile(tableName + "-", suffix);
        tempFile.toFile().deleteOnExit();

        return tempFile;
    }

    private static OutputStream tempFileOutputStream(final Path tempFile,
                                                     final MantaCompressionType compressionType)
            throws IOException, CompressorException {


        OpenOption[] openOptions = new OpenOption[] {StandardOpenOption.CREATE};

        if (compressionType != null) {
            String compressor = compressionType.getCompressorName();
            return COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(compressor,
                    Files.newOutputStream(tempFile, openOptions));
        } else {
            return Files.newOutputStream(tempFile, openOptions);
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
}
