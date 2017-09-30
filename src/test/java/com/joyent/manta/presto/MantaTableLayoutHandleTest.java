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
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

@Test
public class MantaTableLayoutHandleTest {
    private Injector injector;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
    }

    public void canSerializeToAndFromJson() throws IOException {
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        MantaLogicalTable table = new MantaLogicalTable("tablefoo", "/user/dir", MantaDataFileType.NDJSON);
        MantaSchemaTableName tableName = new MantaSchemaTableName("schema", table);
        MantaTableLayoutHandle tableLayoutHandle = new MantaTableLayoutHandle(tableName);
        String json = mapper.writeValueAsString(tableLayoutHandle);
        Assert.assertNotNull(json);

        MantaTableLayoutHandle tableLayoutHandleDeserialized =
                mapper.readValue(json, MantaTableLayoutHandle.class);

        Assert.assertEquals(tableLayoutHandle, tableLayoutHandleDeserialized);
    }
}
