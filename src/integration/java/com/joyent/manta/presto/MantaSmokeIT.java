/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.presto.test.MantaQueryRunner;
import org.apache.commons.compress.compressors.CompressorException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
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

    public void canSelectAllFromSmallTable() throws SQLException {
        try {
            queryRunner.execute(session, "CREATE TABLE memory.default.test_select AS SELECT * FROM nation");

            assertQuery("SELECT * FROM memory.default.test_select ORDER BY nationkey",
                    "SELECT * FROM tpch.tiny.nation ORDER BY nationkey");
        } finally {
            queryRunner.execute(session, "DROP TABLE memory.default.test_select");
        }
    }

    public void canJoinTwoTables() throws SQLException {
        String sql = "SELECT n.name as nation_name, r.name as region_name "
                + "FROM %s.nation AS n JOIN %s.region r ON r.regionkey = n.regionkey"
                + " ORDER BY n.name";

        assertQuery(String.format(sql, "manta.default", "manta.default"),
                    String.format(sql, "tpch.tiny", "tpch.tiny"));
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

    private void assertQuery(String sql, String expected) {
        MaterializedRow[] rows = queryRunner.execute(session, sql)
                .getMaterializedRows().stream().toArray(MaterializedRow[]::new);
        MaterializedRow[] expectedRows = queryRunner.execute(session, expected)
                .getMaterializedRows().stream().toArray(MaterializedRow[]::new);

        Assert.assertEqualsNoOrder(rows, expectedRows);
    }
}
