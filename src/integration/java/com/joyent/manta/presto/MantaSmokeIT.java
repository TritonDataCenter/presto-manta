/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.presto.compression.MantaCompressionType;
import com.joyent.manta.presto.test.MantaQueryRunner;
import org.apache.commons.compress.compressors.CompressorException;
import org.h2.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.UUID;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

@Test
public class MantaSmokeIT {
    private String testPathPrefix;
    private MantaClient mantaClient;
    private DistributedQueryRunner queryRunner;
    private Session session;

    @BeforeClass
    public void before() throws IOException, CompressorException {
        String randomDir = UUID.randomUUID().toString();
        String schemaPath = String.format("~~/stor/java-manta-integration-tests/%s",
                randomDir);
        ConfigContext config = new ChainedConfigContext(
                new EnvVarConfigContext(),
                new MapConfigContext(System.getProperties()),
                new DefaultsConfigContext());
        ConfigContext.validate(config);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s/",
                mantaClient.getContext().getMantaHomeDirectory(), randomDir);
        mantaClient.putDirectory(testPathPrefix, true);

        MantaQueryRunner mantaQueryRunner = new MantaQueryRunner(schemaPath);
        queryRunner = mantaQueryRunner.createQueryRunner();

        MantaQueryRunner.createTestData(testPathPrefix, mantaClient, queryRunner,
                MantaDataFileType.NDJSON, MantaCompressionType.XZ);
    }

    @AfterClass
    public void after() throws IOException {
        if (queryRunner != null) {
            queryRunner.close();
        }

        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    @BeforeMethod
    public void setUp() throws Exception {
        session = testSessionBuilder().setCatalog("manta").setSchema("default").build();
    }

    public void canShowSchemas() {
        MaterializedResult result = queryRunner.execute(session, "SHOW SCHEMAS");
        List<MaterializedRow> rows = result.getMaterializedRows();

        Assert.assertEquals(rows.size(), 2, "Expected two schemas");

        String[] expected = new String[]{"default", "information_schema"};

        String[] actual = rows.stream()
                .map(r -> Objects.toString(r.getField(0)))
                .toArray(String[]::new);

        Assert.assertEqualsNoOrder(actual, expected);
    }

    public void canListTables() throws Exception {
        MaterializedResult result = queryRunner.execute(session, "SHOW TABLES");
        List<MaterializedRow> rows = result.getMaterializedRows();

        String[] expected = new String[]{
                "customer",
                "lineitem",
                "nation",
                "orders",
                "part",
                "partsupp",
                "region",
                "supplier"
        };

        String[] actual = rows.stream()
                .map(r -> Objects.toString(r.getField(0)))
                .toArray(String[]::new);

        Assert.assertEqualsNoOrder(actual, expected);
    }

    public void canSelectAllFromNationTable() throws SQLException {
        String sql = "SELECT * FROM ${schema}.nation ORDER BY nationkey";

        assertQuery(interpolateSchemaInSql(sql, "manta.default"),
                interpolateSchemaInSql(sql, "tpch.tiny"));
    }

    public void canSelectAllFromRegionTable() throws SQLException {
        String sql = "SELECT * FROM ${schema}.region ORDER BY regionkey";

        assertQuery(interpolateSchemaInSql(sql, "manta.default"),
                interpolateSchemaInSql(sql, "tpch.tiny"));
    }

    public void canSelectAllFromCustomerTable() throws SQLException {
        String sql = "SELECT * FROM ${schema}.customer ORDER BY custkey";

        assertQuery(interpolateSchemaInSql(sql, "manta.default"),
                interpolateSchemaInSql(sql, "tpch.tiny"));
    }

    public void canSelectAllFromLineitemTable() throws SQLException {
        String sql = "SELECT * "
                + "FROM ${schema}.lineitem "
                + "ORDER BY orderkey, partkey, suppkey, linenumber, "
                + "shipdate, commitdate, receiptdate";

        assertQuery(interpolateSchemaInSql(sql, "manta.default"),
                interpolateSchemaInSql(sql, "tpch.tiny"));
    }

    public void canSelectAllFromOrderTable() throws SQLException {
        String sql = "SELECT * FROM ${schema}.orders ORDER BY orderkey";

        assertQuery(interpolateSchemaInSql(sql, "manta.default"),
                interpolateSchemaInSql(sql, "tpch.tiny"));
    }

    public void canSelectAllFromPartTable() throws SQLException {
        String sql = "SELECT * FROM ${schema}.part ORDER BY partkey";

        assertQuery(interpolateSchemaInSql(sql, "manta.default"),
                interpolateSchemaInSql(sql, "tpch.tiny"));
    }

    public void canSelectAllFromPartsuppTable() throws SQLException {
        String sql = "SELECT * FROM ${schema}.partsupp ORDER BY partkey, suppkey";

        assertQuery(interpolateSchemaInSql(sql, "manta.default"),
                interpolateSchemaInSql(sql, "tpch.tiny"));
    }

    public void canSelectAllFromSupplierTable() throws SQLException {
        String sql = "SELECT * FROM ${schema}.supplier ORDER BY suppkey";

        assertQuery(interpolateSchemaInSql(sql, "manta.default"),
                interpolateSchemaInSql(sql, "tpch.tiny"));
    }

    public void canJoinTwoTables() throws SQLException {
        String sql = "SELECT n.name as nation_name, r.name as region_name "
                + "FROM ${schema}.nation AS n JOIN ${schema}.region r ON r.regionkey = n.regionkey"
                + " ORDER BY n.name";

        assertQuery(interpolateSchemaInSql(sql, "manta.default"),
                interpolateSchemaInSql(sql, "tpch.tiny"));
    }

    public void testSelectSingleRow() {
        assertQuery("SELECT * FROM nation WHERE nationkey = 1",
                "SELECT * FROM tpch.tiny.nation WHERE nationkey = 1");
    }

    public void testSelectColumnsSubset() throws SQLException {
        assertQuery("SELECT nationkey, regionkey FROM nation ORDER BY nationkey", ""
                + "SELECT nationkey, regionkey FROM tpch.tiny.nation ORDER BY nationkey");
    }

    private void assertQueryResult(String sql, Object... expected) {
        MaterializedResult rows = queryRunner.execute(session, sql);
        assertEquals(rows.getRowCount(), expected.length);

        for (int i = 0; i < expected.length; i++) {
            MaterializedRow materializedRow = rows.getMaterializedRows().get(i);
            int fieldCount = materializedRow.getFieldCount();
            assertEquals(fieldCount, 1,
                    String.format("Expected a single column, but got '%d' columns",
                            fieldCount));
            Object value = materializedRow.getField(0);
            assertEquals(value, expected[i]);
            assertEquals(materializedRow.getFieldCount(), 1);
        }
    }

    private void assertQuery(String sql, String expectedSql) {
        MaterializedResult result = queryRunner.execute(session, sql);
        List<MaterializedRow> materializedRows = result.getMaterializedRows();
        List<Type> types = result.getTypes();

        MaterializedResult expectedResult = queryRunner.execute(session, expectedSql);
        List<MaterializedRow> materializedExpectedRows = expectedResult
                .getMaterializedRows();
        List<Type> expectedTypes = expectedResult.getTypes();

        assertTypeListsAreEqual(types, expectedTypes);

        Assert.assertEquals(result.getRowCount(), expectedResult.getRowCount(),
                "Row counts aren't equal between results");

        ListIterator<MaterializedRow> resultItr = materializedRows.listIterator();
        ListIterator<MaterializedRow> expectedItr = materializedExpectedRows.listIterator();

        int rowNo = 1;
        while (resultItr.hasNext() && expectedItr.hasNext()) {
            final MaterializedRow actual = resultItr.next();
            final MaterializedRow expected = expectedItr.next();

            String[] actualValues = new String[actual.getFields().size()];
            for (int i = 0; i < actualValues.length; i++){
                actualValues[i] = Objects.toString(actual.getField(i), null);
            }

            String[] expectedValues = new String[expected.getFields().size()];
            for (int i = 0; i < expectedValues.length; i++){
                expectedValues[i] = Objects.toString(expected.getField(i), null);
            }

            Assert.assertEquals(actualValues, expectedValues,
                    String.format("Row number %d has different values.\n"
                            + "Expected: %s\n"
                            + "Actual:   %s", rowNo++, expected, actual));
        }
    }

    private String interpolateSchemaInSql(final String sql, final String schemaName) {
        return StringUtils.replaceAll(sql, "${schema}", schemaName);
    }

    private static void assertTypeListsAreEqual(final List<Type> actual,
                                                final List<Type> expected) {
        if (actual == null && expected == null) {
            return;
        }

        Assert.assertNotNull(actual);
        Assert.assertNotNull(expected);

        Assert.assertEquals(actual.size(), expected.size(),
                "Size of type collection differs");

        ListIterator<Type> actualItr = actual.listIterator();
        ListIterator<Type> expectedItr = expected.listIterator();

        int colNo = 1;
        while (actualItr.hasNext() && expectedItr.hasNext()) {
            Type actualType = actualItr.next();
            Type expectedType = expectedItr.next();

            assertTypesAreEqual(actualType, expectedType,
                    String.format("Types differ for column %d", colNo++));
        }
    }

    private static void assertTypesAreEqual(final Type actual, final Type expected,
                                            final String msg) {
        if (expected.getTypeSignature().getBase().equals("date")
                && actual.getTypeSignature().getBase().equals("varchar")) {
            // We don't have a way to coerce dates, so they are processed as strings
            return;
        }

        Class<?> actualJavaType = actual.getJavaType();
        Class<?> expectedJavaType = expected.getJavaType();

        Assert.assertEquals(actualJavaType, expectedJavaType, msg);
    }
}
