/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.presto.MantaPrestoTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.joyent.manta.presto.MantaDataFileType.NDJSON;

@Test
public class MantaLogicalTableProviderIT {
    private Injector injector;
    private ObjectMapper objectMapper;
    private MantaClient mantaClient;
    private String testPathPrefix;


    @BeforeClass
    public void before() throws IOException {
        String randomDir = UUID.randomUUID().toString();

        Map<String, String> emptyConfig = ImmutableMap.of(
                "manta.schema.default", String.format(
                        "~~/stor/java-manta-integration-tests/%s/", randomDir));
        injector = MantaPrestoTestUtils.createInjectorInstance(emptyConfig);

        mantaClient = injector.getInstance(MantaClient.class);
        objectMapper = injector.getInstance(ObjectMapper.class);

        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s/",
                mantaClient.getContext().getMantaHomeDirectory(), randomDir);
        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void after() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    public void canListTablesForDefaultSchema() throws IOException {
        String schemaPath = testPathPrefix + UUID.randomUUID();
        mantaClient.putDirectory(schemaPath);

        List<MantaLogicalTable> tables = ImmutableList.of(
                new MantaLogicalTable("table-1", schemaPath, NDJSON),
                new MantaLogicalTable("table-2", schemaPath, NDJSON),
                new MantaLogicalTable("table-3", schemaPath, NDJSON)
        );

        String definitionJson = objectMapper.writeValueAsString(tables);
        String path = testPathPrefix + MantaLogicalTableProvider.TABLE_DEFINITION_FILENAME;
        mantaClient.put(path, definitionJson, StandardCharsets.UTF_8);

        MantaLogicalTableProvider tableProvider = injector.getInstance(MantaLogicalTableProvider.class);

        List<MantaLogicalTable> actual = ImmutableList.of(
            tableProvider.getTable("default", "table-1"),
            tableProvider.getTable("default", "table-2"),
            tableProvider.getTable("default", "table-3")
        );

        Assert.assertEqualsNoOrder(actual.toArray(), tables.toArray());
    }
}
