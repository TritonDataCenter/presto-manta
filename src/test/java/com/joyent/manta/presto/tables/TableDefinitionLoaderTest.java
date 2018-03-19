/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.joyent.manta.presto.MantaPrestoTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

@Test
public class TableDefinitionLoaderTest {
    private Injector injector;
    private ObjectMapper mapper;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
        mapper = injector.getInstance(ObjectMapper.class);
    }

    private class TestTableDefinitionLoader extends TableDefinitionLoader {
        private final URL testURL;

        public TestTableDefinitionLoader(final ObjectMapper objectMapper,
                                         final URL testURL) {
            super(objectMapper);
            this.testURL = Objects.requireNonNull(testURL);
        }

        @Override
        InputStream openStream() throws IOException {
            return testURL.openStream();
        }
    }

    public void canLoadExamplePrestoTableDefinition() throws Exception {
        ClassLoader classLoader = this.getClass().getClassLoader();
        String path = "test-data/logical-table-definition/presto-tables-example.json";
        URL url = classLoader.getResource(path);

        final TableDefinitionLoader loader = new TestTableDefinitionLoader(
                mapper, url);

        final Map<String, MantaLogicalTable> tables = loader.call();
    }
}
