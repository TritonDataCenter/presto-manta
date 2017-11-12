/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.telegraf;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Injector;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.MantaMetadata;
import com.joyent.manta.presto.MantaRecordSetProvider;
import com.joyent.manta.presto.MantaSplitManager;
import com.joyent.manta.presto.MantaTableLayoutHandle;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaLogicalTableProvider;
import com.joyent.manta.presto.tables.MantaSchemaTableName;
import com.joyent.manta.presto.test.MantaPrestoIntegrationTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.joyent.manta.presto.test.MantaPrestoIntegrationTestUtils.setupConfiguration;

//@Test
public class MantaTelegrafJsonRecordCursorIT {
    private Injector injector;
    private MantaClient mantaClient;
    private MantaMetadata metadata;
    private String testPathPrefix;
    private ConnectorSession session;

    @BeforeClass
    public void before() throws IOException {
        MantaPrestoIntegrationTestUtils.IntegrationSetup setup = setupConfiguration();
        injector = setup.injector;
        mantaClient = setup.mantaClient;
        metadata = setup.instance;
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

    public void canParseDateValues() throws IOException {
        final ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("date-field0", "07-01-2014");
        node.put("date-field1", "1980-08-10");
        node.put("date-field2", "09/09/2047");
        node.put("date-field3", "1977/11/22");
        node.put("date-field4", "02 Oct 1944");
        node.put("date-field5", "13 December 1124");

        final Function<Block[], Void> assertion = blocks -> {
            String field0 = DateType.DATE.getObjectValue(session, blocks[0], 0).toString();
            Assert.assertEquals(field0, LocalDate.of(2014, 1, 7).toString());

            String field1 = DateType.DATE.getObjectValue(session, blocks[1], 0).toString();
            Assert.assertEquals(field1, LocalDate.of(1980, 8, 10).toString());

            String field2 = DateType.DATE.getObjectValue(session, blocks[2], 0).toString();
            Assert.assertEquals(field2, LocalDate.of(2047, 9, 9).toString());

            String field3 = DateType.DATE.getObjectValue(session, blocks[3], 0).toString();
            Assert.assertEquals(field3, LocalDate.of(1977, 11, 22).toString());
            String field4 = DateType.DATE.getObjectValue(session, blocks[4], 0).toString();
            Assert.assertEquals(field4, LocalDate.of(1944, 10, 2).toString());

            String field5 = DateType.DATE.getObjectValue(session, blocks[5], 0).toString();
            Assert.assertEquals(field5, LocalDate.of(1124, 12, 13).toString());

            return null;
        };

        validateSingleRow(node, assertion);
    }

    public void canParseStringValues() throws IOException {
        final ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("field0", "This is a string"); // String
        node.put("field1", "これもストリング"); // Unicode String

        final Function<Block[], Void> assertion = blocks -> {
            String field0 = VarcharType.VARCHAR.getSlice(blocks[0], 0).toStringUtf8();
            Assert.assertEquals(field0, node.get("field0").asText());

            String field1 = VarcharType.VARCHAR.getSlice(blocks[1], 0).toStringUtf8();
            Assert.assertEquals(field1, node.get("field1").asText());

            return null;
        };

        validateSingleRow(node, assertion);
    }

    public void canParseNumericValues() throws IOException {
        final ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("field0", 1); // Integer
        node.put("field1", Long.MIN_VALUE); // Long
        BigDecimal bigValue = BigDecimal.valueOf(Long.MAX_VALUE, 10).pow(2);
        node.put("field2", bigValue); // Bigdecimal
        node.put("field3",  3.14159); // Float
        node.put("field4", 2.718281828459D); // Double

        final Function<Block[], Void> assertion = blocks -> {
            // All ints are cast to long within Presto
            long field0 = BigintType.BIGINT.getLong(blocks[0], 0);
            Assert.assertEquals(field0, node.get("field0").asLong());

            long field1 = BigintType.BIGINT.getLong(blocks[1], 0);
            Assert.assertEquals(field1, node.get("field1").asLong());

            double field2 = DoubleType.DOUBLE.getDouble(blocks[2],0);
            Assert.assertEquals(field2, node.get("field2").asDouble());

            double field3 = DoubleType.DOUBLE.getDouble(blocks[3], 0);
            Assert.assertEquals(field3, node.get("field3").asDouble());

            double field4 = DoubleType.DOUBLE.getDouble(blocks[4], 0);
            Assert.assertEquals(field4, node.get("field4").asDouble());

            return null;
        };

        validateSingleRow(node, assertion);
    }

    private void validateSingleRow(final ObjectNode node, Function<Block[], Void> assertionBlock)
            throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        MantaDataFileType dataFileType = MantaDataFileType.TELEGRAF_NDJSON;
        MantaLogicalTable table = new MantaLogicalTable("singlerow",
                testPathPrefix, dataFileType);
        {
            String tablePath = testPathPrefix + MantaLogicalTableProvider.TABLE_DEFINITION_FILENAME;
            MantaHttpHeaders headers = new MantaHttpHeaders().setContentType("application/json");

            try (OutputStream out = mantaClient.putAsOutputStream(tablePath, headers)) {
                mapper.writeValue(out, table);
            }
        }

        String dataFilePath = testPathPrefix + "data.telegraf.json";

        {
            MantaHttpHeaders headers = new MantaHttpHeaders()
                    .setContentType(dataFileType.getDefaultMediaType());
            byte[] data = mapper.writeValueAsBytes(node);

            mantaClient.put(dataFilePath, data, headers);
        }

        MantaSchemaTableName schemaTableName = new MantaSchemaTableName("default", table);

        List<? extends ColumnHandle> columns = metadata.getColumnHandles(session, schemaTableName)
                .entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

        MantaRecordSetProvider recordSetProvider = injector.getInstance(MantaRecordSetProvider.class);
        ConnectorTransactionHandle transactionHandle = new TestingTransactionHandle(UUID.randomUUID());
        MantaSplitManager splitManager = injector.getInstance(MantaSplitManager.class);
        MantaTableLayoutHandle tableLayoutHandle = new MantaTableLayoutHandle(schemaTableName);
        ConnectorSplit split;

        try (ConnectorSplitSource splitSource = splitManager.getSplits(transactionHandle, session, tableLayoutHandle)) {
            List<ConnectorSplit> splits = splitSource.getNextBatch(10).get();
            Assert.assertEquals(splits.size(), 1,
                    "There should be only one split for this test");
            split = splits.get(0);
        } catch (InterruptedException | ExecutionException e) {
            throw new AssertionError("Problem processing split source", e);
        }
        RecordSet recordSet = recordSetProvider.getRecordSet(transactionHandle, session, split, columns);

        try (RecordPageSource recordPageSource = new RecordPageSource(recordSet)) {
            Page page = recordPageSource.getNextPage();
            Block[] blocks = page.getBlocks();

            Assert.assertEquals(blocks.length, node.size(),
                    "Number of blocks doesn't match number of fields on node");

            assertionBlock.apply(blocks);

            Assert.assertTrue(recordPageSource.isFinished(),
                    "There should be no more pages");
        }
    }
}
