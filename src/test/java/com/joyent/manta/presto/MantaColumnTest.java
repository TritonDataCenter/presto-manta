/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.types.MapStringType;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

@Test
public class MantaColumnTest {
    private Injector injector;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
    }

    public void canSerializeToAndFromJsonWithVarCharType() throws IOException {
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        MantaColumn column = new MantaColumn("test",
                VarcharType.VARCHAR, "test");
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(column);

        Assert.assertNotNull(json);

        MantaColumn columnDeserialized = mapper.readValue(json, MantaColumn.class);
        Assert.assertEquals(column, columnDeserialized);
    }

    public void canSerializeToAndFromJsonWithMapStringStringType() throws IOException {
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        MantaColumn column = new MantaColumn("test",
                MapStringType.MAP_STRING_STRING, "mapStringString");
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(column);

        Assert.assertNotNull(json);

        MantaColumn columnDeserialized = mapper.readValue(json, MantaColumn.class);
        Assert.assertEquals(column, columnDeserialized);
    }

    public void canSerializeToAndFromJsonWithMapStringDoubleType() throws IOException {
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        MantaColumn column = new MantaColumn("test",
                MapStringType.MAP_STRING_DOUBLE, "mapStringDouble");
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(column);

        Assert.assertNotNull(json);

        MantaColumn columnDeserialized = mapper.readValue(json, MantaColumn.class);
        Assert.assertEquals(column, columnDeserialized);
    }

    public void canSerializeToAndFromJsonWithTimestampEpochMilliseconds() throws IOException {
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        MantaColumn column = new MantaColumn("test",
                TimestampType.TIMESTAMP, "timestampEpochMilliseconds");
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(column);

        Assert.assertNotNull(json);

        MantaColumn columnDeserialized = mapper.readValue(json, MantaColumn.class);
        Assert.assertEquals(column, columnDeserialized);
    }
}
