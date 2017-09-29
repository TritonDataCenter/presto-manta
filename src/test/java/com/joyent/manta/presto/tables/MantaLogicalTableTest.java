/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.MantaPrestoTestUtils;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.regex.Pattern;

@Test
public class MantaLogicalTableTest {
    private Injector injector;
    private ObjectMapper mapper;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
        mapper = injector.getInstance(ObjectMapper.class);
    }

    public void canSerializeFromJsonWithNullFilters() throws IOException {
        String input = "{ \"name\":\"logical-table-1\", \"rootPath\":\"/user/stor/json-examples\", \"dataFileType\":\"NDJSON\",  \"directoryFilterRegex\":\"\", \"filterRegex\":\"\" }";

        MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                "/user/stor/json-examples",
                MantaDataFileType.NDJSON);
        MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
        Assert.assertEquals(expected, actual);
    }

    public void canSerializeFromJsonWithDirectoryFilter() throws IOException {
        String input = "{ \"name\":\"logical-table-1\", \"rootPath\":\"/user/stor/json-examples\", \"dataFileType\":\"NDJSON\",  \"directoryFilterRegex\":\"^.*dir2\\/.*\\\\.json$\", \"filterRegex\":\"\" }";

        MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                "/user/stor/json-examples",
                MantaDataFileType.NDJSON,
                Pattern.compile("^.*dir2/.*\\.json$"), null);
        MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
        Assert.assertEquals(expected, actual);
    }

    public void canSerializeFromJsonWithFilter() throws IOException {
        String input = "{ \"name\":\"logical-table-1\", \"rootPath\":\"/user/stor/json-examples\", \"dataFileType\":\"NDJSON\", \"directoryFilterRegex\":\"\", \"filterRegex\":\"^.*\\\\.json$\" }";

        MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                "/user/stor/json-examples",
                MantaDataFileType.NDJSON,
                null, Pattern.compile("^.*\\.json$"));
        MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
        Assert.assertEquals(expected, actual);
    }

    public void canSerializeFromJsonWithMissingFilters() throws IOException {
        String input = "{ \"name\":\"logical-table-1\", \"rootPath\":\"/user/stor/json-examples\", \"dataFileType\":\"NDJSON\" }";

        MantaLogicalTable expected = new MantaLogicalTable("logical-table-1",
                "/user/stor/json-examples",
                MantaDataFileType.NDJSON);
        MantaLogicalTable actual = mapper.readValue(input, MantaLogicalTable.class);
        Assert.assertEquals(expected, actual);
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithBlankName() throws IOException {
        String input = "{ \"name\":\"\", \"rootPath\":\"/user/stor/json-examples\", \"dataFileType\":\"json\" }";
        mapper.readValue(input, MantaLogicalTable.class);
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithMissingName() throws IOException {
        String input = "{ \"rootPath\":\"/user/stor/json-examples\", \"dataFileType\":\"json\" }";
        mapper.readValue(input, MantaLogicalTable.class);
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithNullName() throws IOException {
        String input = "{ \"name\":null, \"rootPath\":\"/user/stor/json-examples\", \"dataFileType\":\"json\" }";
        mapper.readValue(input, MantaLogicalTable.class);
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithBlankRootPath() throws IOException {
        String input = "{ \"name\":\"logical-table-1\", \"rootPath\":\"\", \"dataFileType\":\"json\" }";
        mapper.readValue(input, MantaLogicalTable.class);
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithMissingRootPath() throws IOException {
        String input = "{ \"name\":\"logical-table-1\", \"dataFileType\":\"json\" }";
        mapper.readValue(input, MantaLogicalTable.class);
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithNullRootPath() throws IOException {
        String input = "{ \"name\":\"logical-table-1\",  \"rootPath\":null, \"dataFileType\":\"json\" }";
        mapper.readValue(input, MantaLogicalTable.class);
    }

    @Test(expectedExceptions = JsonMappingException.class)
    public void cantSerializeFromJsonWithBadPattern() throws IOException {
        String input = "{ \"name\":\"logical-table-1\", \"rootPath\":\"/user/stor/json-examples\", \"dataFileType\":\"json\", \"directoryFilterRegex\":\"**dir2\\/.*\\\\.json$\" }";
        mapper.readValue(input, MantaLogicalTable.class);
    }
}
