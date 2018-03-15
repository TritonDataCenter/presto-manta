/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.joyent.manta.presto.column.MantaPartitionColumn;
import com.joyent.manta.presto.tables.MantaLogicalTablePartitionDefinition;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

@Test
public class MantaSplitPartitionPredicateTest {
    private Injector injector;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
    }

    public void canSerializeToAndFromJson() throws IOException {
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);


        MantaLogicalTablePartitionDefinition partitionDefinition =
                MantaSplitManagerTest.createPartitionDefinition();

        LinkedHashMap<MantaPartitionColumn, String> partitionColumnToMatchValue =
                new LinkedHashMap<>();

        List<MantaPartitionColumn> partitionColumns = partitionDefinition.filePartitionsAsColumns();

        partitionColumnToMatchValue.put(partitionColumns.get(0), "year");

        MantaSplitPartitionPredicate predicate = new MantaSplitPartitionPredicate(
                partitionColumnToMatchValue, partitionDefinition.getFilterRegex());

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(predicate);
        Assert.assertNotNull(json);

        try {
            MantaSplitPartitionPredicate predicateDeserialized =
                    mapper.readValue(json, MantaSplitPartitionPredicate.class);
            Assert.assertEquals(predicate, predicateDeserialized);
        } catch (JsonMappingException | AssertionError e) {
            System.err.println(json);
            throw e;
        }
    }
}
