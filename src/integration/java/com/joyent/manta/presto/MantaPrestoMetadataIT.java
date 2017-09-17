/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.presto.exceptions.MantaPrestoSchemaNotFoundException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.mock;

@Test
public class MantaPrestoMetadataIT {
    private Injector injector;
    private MantaClient mantaClient;
    private MantaPrestoMetadata instance;
    private String testPathPrefix;
    private ConnectorSession session;

    @BeforeClass
    public void before() throws IOException {
        Map<String, String> emptyConfig = Collections.emptyMap();
        injector = MantaPrestoTestUtils.createInjectorInstance(emptyConfig);

        mantaClient = injector.getInstance(MantaClient.class);
        instance = injector.getInstance(MantaPrestoMetadata.class);
        session = mock(ConnectorSession.class);

        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s/",
                mantaClient.getContext().getMantaHomeDirectory(), UUID.randomUUID());

        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void after() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    public void canListSchemaNames() {

        List<String> schemas = instance.listSchemaNames(session);

        ReflectionAssert.assertLenientEquals(
                "Schema's returned from Manta didn't match expected defaults",
                MantaPrestoMetadata.DEFAULT_SCHEMAS, schemas);
    }

    public void canListTablesForEmptyDirectory() throws IOException {
        final String testDir = testPathPrefix + UUID.randomUUID();
        mantaClient.putDirectory(testDir);

        List<SchemaTableName> tables = instance.listTables(session, testDir);
        Assert.assertTrue(tables.isEmpty(),
                "Expected no tables listed. Actually there are " + tables.size() + " tables.");
    }

    public void canListTablesWithValidExtension() throws IOException {
        final String testDir = testPathPrefix + UUID.randomUUID();
        mantaClient.putDirectory(testDir);

        List<MantaPrestoSchemaTableName> expected = ImmutableList.of(
                new MantaPrestoSchemaTableName(testDir, "file-1.ndjson"),
                new MantaPrestoSchemaTableName(testDir, "file-2.ndjson"),
                new MantaPrestoSchemaTableName(testDir, "file-3.ndjson")
        );

        for (MantaPrestoSchemaTableName table : expected) {
            mantaClient.put(table.getObjectPath(), table.getFile() + " content", UTF_8);
        }

        List<SchemaTableName> actual = instance.listTables(session, testDir);

        ReflectionAssert.assertLenientEquals(
                "Tables returned from Manta didn't match files added during test",
                expected, actual);
    }

    public void canListTablesWithValidMediaType() throws IOException {
        final String testDir = testPathPrefix + UUID.randomUUID();
        mantaClient.putDirectory(testDir);

        List<MantaPrestoSchemaTableName> expected = ImmutableList.of(
                new MantaPrestoSchemaTableName(testDir, "file-1"),
                new MantaPrestoSchemaTableName(testDir, "file-2"),
                new MantaPrestoSchemaTableName(testDir, "file-3")
        );

        for (MantaPrestoSchemaTableName table : expected) {
            MantaHttpHeaders headers = new MantaHttpHeaders()
                    .setContentType("application/x-ndjson; charset=utf8");
            mantaClient.put(table.getObjectPath(), table.getFile() + " content", headers);
        }

        List<SchemaTableName> actual = instance.listTables(session, testDir);

        ReflectionAssert.assertLenientEquals(
                "Tables returned from Manta didn't match files added during test",
                expected, actual);
    }

    public void willErrorNicelyIfSchemaIsNotFound() {
        final String badDir = testPathPrefix + UUID.randomUUID() + "-not-found";

        boolean thrown = false;

        try {
            instance.listTables(session, badDir);
        } catch (MantaPrestoSchemaNotFoundException e) {
            thrown = true;
            Assert.assertEquals(e.getFirstContextValue("schemaPath"), badDir);
        }

        Assert.assertTrue(thrown,
                "Expected exception MantaPrestoSchemaNotFoundException wasn't thrown");
    }
}
