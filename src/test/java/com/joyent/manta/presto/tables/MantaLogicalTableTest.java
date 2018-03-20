/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.JsonType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.MantaPrestoTestUtils;
import com.joyent.manta.presto.column.MantaColumn;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Pattern;

@Test
public class MantaLogicalTableTest {
    private final String basePath = "test-data/logical-table-definition/";
    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    private Injector injector;
    private ObjectMapper mapper;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
        mapper = injector.getInstance(ObjectMapper.class);
    }

    public void canDeserializeFromJsonWithNullFilters() throws IOException {
        final String resourcePath = basePath + "null-filters.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                    "/user/stor/json-examples",
                    MantaDataFileType.NDJSON);
            MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
            Assert.assertEquals(expected, actual);
        }
    }

    public void canDeserializeFromJsonWithDirectoryFilter() throws IOException {
        final String resourcePath = basePath + "with-directory-filters.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            MantaLogicalTablePartitionDefinition partitionDefinition =
                    new MantaLogicalTablePartitionDefinition(
                            Pattern.compile("^.*dir2/.*\\.json$"), null,
                            new LinkedHashSet<>(), new LinkedHashSet<>()
                    );

            MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                    "/user/stor/json-examples",
                    MantaDataFileType.NDJSON,
                    partitionDefinition);
            MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
            Assert.assertEquals(expected, actual);
        }
    }

    public void canDeserializeFromJsonWithDirectoryFilterAndPartitions() throws IOException {
        final String resourcePath = basePath + "with-directory-filters-partitions.json";
        final LinkedHashSet<String> dirPartitions = new LinkedHashSet<>();
        dirPartitions.add("year");
        dirPartitions.add("month");
        dirPartitions.add("day");

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            MantaLogicalTablePartitionDefinition partitionDefinition =
                    new MantaLogicalTablePartitionDefinition(
                            Pattern.compile("^/user/stor/json-examples/(.+)/(.+)/(.+)/.+\\.json$"), null,
                            dirPartitions, new LinkedHashSet<>()
                    );

            MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                    "/user/stor/json-examples",
                    MantaDataFileType.NDJSON,
                    partitionDefinition);
            MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
            Assert.assertEquals(expected, actual);
        }
    }

    public void canDeserializeFromJsonWithFilter() throws IOException {
        final String resourcePath = basePath + "with-file-filters.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            MantaLogicalTablePartitionDefinition partitionDefinition =
                    new MantaLogicalTablePartitionDefinition(
                            null, Pattern.compile("^.*\\.json$"),
                            new LinkedHashSet<>(), new LinkedHashSet<>()
                    );

            MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                    "/user/stor/json-examples",
                    MantaDataFileType.NDJSON, partitionDefinition);
            MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
            Assert.assertEquals(expected, actual);
        }
    }

    public void canDeserializeFromJsonWithFilterAndPartitions() throws IOException {
        final String resourcePath = basePath + "with-file-filters-partitions.json";
        final LinkedHashSet<String> partitions = new LinkedHashSet<>();
        partitions.add("year");
        partitions.add("month");
        partitions.add("day");

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            MantaLogicalTablePartitionDefinition partitionDefinition =
                    new MantaLogicalTablePartitionDefinition(
                            null, Pattern.compile(
                                    "^/user/stor/json-examples/(.+)-(.+)-(.+)-.+\\.json$"),
                            new LinkedHashSet<>(), partitions
                    );

            MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                    "/user/stor/json-examples",
                    MantaDataFileType.NDJSON,
                    partitionDefinition);
            MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
            Assert.assertEquals(expected, actual);
        }
    }

    public void canDeserializeFromJsonWithMissingFilters() throws IOException {
        final String resourcePath = basePath + "missing-filters.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                    "/user/stor/json-examples",
                    MantaDataFileType.NDJSON);
            MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
            Assert.assertEquals(expected, actual);
        }
    }

    public void canDeserializeFromJsonWithEmptyFilters() throws IOException {
        final String resourcePath = basePath + "empty-filters.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                    "/user/stor/json-examples",
                    MantaDataFileType.NDJSON);
            MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
            Assert.assertEquals(expected, actual);
        }
    }

    public void canDeserializeFromJsonWithSpecifiedColumnsAndFormats() throws IOException {
        final String resourcePath = basePath + "with-columns-and-format.json";

        final List<MantaColumn> expectedColumns = ImmutableList.of(
                new MantaColumn("name", VarcharType.VARCHAR, null),
                new MantaColumn("timestamp-iso8601", TimestampType.TIMESTAMP, null, "[timestamp] iso-8601", false),
                new MantaColumn("timestamp-epoch-seconds", TimestampType.TIMESTAMP, null, "[timestamp] epoch-seconds", false),
                new MantaColumn("timestamp-epoch-milliseconds", TimestampType.TIMESTAMP, null, "[timestamp] epoch-milliseconds", false),
                new MantaColumn("timestamp-epoch-days", TimestampType.TIMESTAMP, null, "[timestamp] epoch-days", false),
                new MantaColumn("timestamp-default", TimestampType.TIMESTAMP, null),
                new MantaColumn("date", DateType.DATE, null, "[date] yyyy-MM-dd", false),
                new MantaColumn("count", IntegerType.INTEGER, null),
                new MantaColumn("properties", JsonType.JSON, null)
        );

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                    "/user/stor/json-examples",
                    MantaDataFileType.NDJSON,
                    null,
                    expectedColumns);
            MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
            Assert.assertEquals(expected, actual);
            Assert.assertEquals(expectedColumns, actual.getColumns());
        }
    }

    public void canSerializeWithUnconfiguredMapperAndDeserializeWithCustomDeserializerMapper() throws IOException {
        final ObjectMapper unconfiguredMapper = new ObjectMapper();
        final ObjectMapper customizedMapper = mapper;

        canSerializeAndDeserializeAllFieldsWithMapper(unconfiguredMapper, customizedMapper);
    }

    public void canSerializeWithUnconfiguredMapperAndDeserializeWithUnconfiguredMapper() throws IOException {
        final ObjectMapper unconfiguredMapper = new ObjectMapper();

        canSerializeAndDeserializeAllFieldsWithMapper(unconfiguredMapper, unconfiguredMapper);
    }

    public void canSerializeWithCustomMapperAndDeserializeWithUnconfiguredMapper() throws IOException {
        final ObjectMapper unconfiguredMapper = new ObjectMapper();
        final ObjectMapper customizedMapper = mapper;

        canSerializeAndDeserializeAllFieldsWithMapper(customizedMapper, unconfiguredMapper);
    }

    public void canSerializeWithCustomMapperAndDeserializeWithCustomMapper() throws IOException {
        final ObjectMapper customizedMapper = mapper;

        canSerializeAndDeserializeAllFieldsWithMapper(customizedMapper, customizedMapper);
    }

    private static void canSerializeAndDeserializeAllFieldsWithMapper(final ObjectMapper serializeMapper,
                                                                      final ObjectMapper deserializeMapper) throws IOException {


        final Pattern directoryFilterRegex = Pattern.compile("^/user/stor/json-examples/(.+)/.+\\\\.json$");
        final Pattern filterRegex = Pattern.compile("^/user/stor/json-examples/.+/(.+)-(.+)-(.+)-.+\\\\.json$");
        final LinkedHashSet<String> directoryFilterPartitions = new LinkedHashSet<>();
        directoryFilterPartitions.add("server");

        final LinkedHashSet<String> filterPartitions = new LinkedHashSet<>();
        filterPartitions.add("year");
        filterPartitions.add("month");
        filterPartitions.add("day");

        final MantaLogicalTablePartitionDefinition partitionDefinition =
                new MantaLogicalTablePartitionDefinition(directoryFilterRegex, filterRegex,
                        directoryFilterPartitions, filterPartitions);
        final List<MantaColumn> columns = ImmutableList.of(
                new MantaColumn("name", VarcharType.VARCHAR, null),
                new MantaColumn("timestamp-iso8601", TimestampType.TIMESTAMP, null, "[timestamp] iso-8601", false),
                new MantaColumn("timestamp-epoch-seconds", TimestampType.TIMESTAMP, null, "[timestamp] epoch-seconds", false),
                new MantaColumn("timestamp-epoch-milliseconds", TimestampType.TIMESTAMP, null, "[timestamp] epoch-milliseconds", false),
                new MantaColumn("timestamp-epoch-days", TimestampType.TIMESTAMP, null, "[timestamp] epoch-days", false),
                new MantaColumn("timestamp-default", TimestampType.TIMESTAMP, null),
                new MantaColumn("date", DateType.DATE, null, "[date] yyyy-MM-dd", false),
                new MantaColumn("count", IntegerType.INTEGER, null),
                new MantaColumn("properties", JsonType.JSON, "Free form JSON data", null, true)
        );

        final MantaLogicalTable expected = new MantaLogicalTable(
                "test-table", "/user/stor/root/path", MantaDataFileType.NDJSON,
                partitionDefinition, columns);

        final String json = serializeMapper.writerWithDefaultPrettyPrinter().writeValueAsString(expected);
        final MantaLogicalTable actual = deserializeMapper.readValue(json, MantaLogicalTable.class);

        try {
            Assert.assertEquals(expected, actual);
            Assert.assertEquals(columns, actual.getColumns());
        } catch (AssertionError e) {
            System.err.println(json);
            throw e;
        }
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithBlankName() throws IOException {
        final String resourcePath = basePath + "blank-name.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            mapper.readValue(input, MantaLogicalTable.class);
        }
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithMissingName() throws IOException {
        final String resourcePath = basePath + "missing-name.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            mapper.readValue(input, MantaLogicalTable.class);
        }
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithNullName() throws IOException {
        final String resourcePath = basePath + "null-name.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            mapper.readValue(input, MantaLogicalTable.class);
        }
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithBlankRootPath() throws IOException {
        final String resourcePath = basePath + "blank-root-path.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            mapper.readValue(input, MantaLogicalTable.class);
        }
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithMissingRootPath() throws IOException {
        final String resourcePath = basePath + "missing-root-path.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            mapper.readValue(input, MantaLogicalTable.class);
        }
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithNullRootPath() throws IOException {
        final String resourcePath = basePath + "null-root-path.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            mapper.readValue(input, MantaLogicalTable.class);
        }
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithBadPattern() throws IOException {
        final String resourcePath = basePath + "bad-pattern.json";

        try (InputStream input = classLoader.getResourceAsStream(resourcePath)) {
            mapper.readValue(input, MantaLogicalTable.class);
        }
    }
}
