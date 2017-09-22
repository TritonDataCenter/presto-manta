/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.joyent.manta.client.MantaClient;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test
public class MantaModuleTest {
    private Injector injector;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
    }

    public void canReadFromConfigAndUpdateSchemaMapping() {
        Map<String, String> config = ImmutableMap.of(
                "manta.user", "user",
                "manta.schema.default", "/user/stor/default-directory",
                "manta.schema.another", "/user/stor/a/b/another",
                "manta.schema.relative", "~~/stor/relative"
        );
        Map<String, String> schemaMapping = new HashMap<>();

        MantaModule.addToSchemaMapping(config, schemaMapping, "/user");

        Assert.assertTrue(schemaMapping.size() == 3,
                "Expected only a schema map with 3 entries. Actually: "
                        + schemaMapping.size() + ".");

        Assert.assertEquals(
                schemaMapping.get("default"),
                config.get("manta.schema.default"),
                "Schema mapping for 'default' schema not created");

        Assert.assertEquals(
                schemaMapping.get("another"),
                config.get("manta.schema.another"),
                "Schema mapping for 'another' schema not created");

        Assert.assertEquals(
                schemaMapping.get("relative"),
                "/user/stor/relative",
                "Schema mapping for 'another' schema not created");
    }

    public void verifyMantaClientInstancesAreTheSameInstance() {
        MantaClient instance1 = injector.getInstance(MantaClient.class);
        MantaClient instance2 = injector.getInstance(MantaClient.class);

        Assert.assertSame(instance1, instance2,
                "Dependency injection is generating a different instance "
                        + "per getInstance() call");
    }
}
