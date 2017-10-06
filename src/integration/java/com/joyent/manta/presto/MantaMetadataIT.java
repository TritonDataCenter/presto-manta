/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.presto.exceptions.MantaPrestoSchemaNotFoundException;
import com.joyent.manta.presto.test.MantaPrestoIntegrationTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.io.IOException;
import java.util.List;

import static com.joyent.manta.presto.test.MantaPrestoIntegrationTestUtils.setupConfiguration;

@Test
public class MantaMetadataIT {
    private MantaClient mantaClient;
    private MantaMetadata instance;
    private String testPathPrefix;
    private ConnectorSession session;

    @BeforeClass
    public void before() throws IOException {
        MantaPrestoIntegrationTestUtils.IntegrationSetup setup = setupConfiguration();
        mantaClient = setup.mantaClient;
        instance = setup.instance;
        session = setup.session;
        testPathPrefix = setup.testPathPrefix;
    }

    @AfterClass
    public void after() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    @AfterMethod
    public void cleanUp() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.putDirectory(testPathPrefix, true);
        }
    }

    public void canListSchemaNames() {

        List<String> schemas = instance.listSchemaNames(session);
        List<String> expected = ImmutableList.of("default");

        ReflectionAssert.assertLenientEquals(
                "Schema's returned from Manta didn't match expected defaults",
                expected, schemas);
    }

    public void willErrorNicelyIfSchemaIsNotFound() {
        final String badSchema = "this-schema-is-not-found";

        boolean thrown = false;

        try {
            instance.listTables(session, badSchema);
        } catch (MantaPrestoSchemaNotFoundException e) {
            thrown = true;
        }

        Assert.assertTrue(thrown,
                "Expected exception MantaPrestoSchemaNotFoundException wasn't thrown");
    }
}
