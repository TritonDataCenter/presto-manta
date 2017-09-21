/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

@Test
public class MantaPrestoTableLayoutHandleTest {
    private Injector injector;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
    }

    public void canSerializeToAndFromJson() throws IOException {
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        MantaPrestoSchemaTableName tableName = new MantaPrestoSchemaTableName("schema", "table",
                "/user/stor/foo", "table.json");
        MantaPrestoTableLayoutHandle tableLayoutHandle = new MantaPrestoTableLayoutHandle(tableName);
        String json = mapper.writeValueAsString(tableLayoutHandle);
        Assert.assertNotNull(json);

        MantaPrestoTableLayoutHandle tableLayoutHandleDeserialized =
                mapper.readValue(json, MantaPrestoTableLayoutHandle.class);

        Assert.assertEquals(tableLayoutHandle, tableLayoutHandleDeserialized);
    }
}
