/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.joyent.manta.presto.column.MantaPrestoColumn;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

@Test
public class MantaPrestoColumnTest {
    private Injector injector;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
    }

    public void canSerializeToAndFromJson() throws IOException {
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        MantaPrestoColumn column = new MantaPrestoColumn("test",
                VarcharType.VARCHAR, "test");
        String json = mapper.writeValueAsString(column);

        Assert.assertNotNull(json);

//        MantaPrestoColumn columnDeserialized = mapper.readValue(json, MantaPrestoColumn.class);
//        Assert.assertEquals(column, columnDeserialized);
    }
}
