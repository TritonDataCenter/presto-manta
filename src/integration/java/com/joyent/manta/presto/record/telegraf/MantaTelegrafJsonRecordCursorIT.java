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
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.TimestampType;
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
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.joyent.manta.presto.test.MantaPrestoIntegrationTestUtils.setupConfiguration;
import static com.joyent.manta.presto.types.MapStringType.MAP_STRING_DOUBLE;
import static com.joyent.manta.presto.types.MapStringType.MAP_STRING_STRING;

@Test
public class MantaTelegrafJsonRecordCursorIT {
    private Injector injector;
    private MantaClient mantaClient;
    private MantaMetadata metadata;
    private String testPathPrefix;
    private Supplier<ConnectorSession> sessionSupplier;

    @BeforeClass
    public void before() throws IOException {
        MantaPrestoIntegrationTestUtils.IntegrationSetup setup = setupConfiguration();
        injector = setup.injector;
        mantaClient = setup.mantaClient;
        metadata = setup.instance;
        sessionSupplier = setup.sessionSupplier;
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

    @SuppressWarnings("unchecked")
    public void canParseMapValues() throws IOException {
        final ConnectorSession session = sessionSupplier.get();

        final ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        final long timestampSeconds = 1496275200;
        node.put("timestamp", timestampSeconds);

        {
            final ObjectNode tags = new ObjectNode(JsonNodeFactory.instance);
            tags.put("arch", "x64");
            tags.put("datacenter", "ap-southeast-1b");
            node.set("tags", tags);
        }

        node.put("name", "cpu");

        {
            final ObjectNode fields = new ObjectNode(JsonNodeFactory.instance);
            fields.put("usage_guest", 94.59371357640609);
            fields.put("usage_guest_nice", 58.79378775101397);
            node.set("fields", fields);
        }

        final Function<Block[], Void> assertion = blocks -> {
            SqlTimestamp timestamp = (SqlTimestamp)TimestampType.TIMESTAMP.getObjectValue(session, blocks[0], 0);
            Assert.assertEquals(timestamp.getMillisUtc(), timestampSeconds * 1_000L,
                    "Epoch seconds was not converted to epoch milliseconds");

            Map<String, String> tags = (Map<String, String>)MAP_STRING_STRING.getObjectValue(session, blocks[1], 0);
            Assert.assertEquals(tags.size(), 2, "Map value was unexpected length");
            Assert.assertEquals(tags.get("arch"), "x64");
            Assert.assertEquals(tags.get("datacenter"), "ap-southeast-1b");

            String name = VarcharType.VARCHAR.getSlice(blocks[2], 0).toStringUtf8();
            Assert.assertEquals(name, "cpu");

            Map<String, Double> fields = (Map<String, Double>)MAP_STRING_DOUBLE.getObjectValue(session, blocks[3], 0);
            Assert.assertEquals(fields.size(), 2, "Map value was unexpected length");
            Assert.assertEquals(fields.get("usage_guest"), 94.59371357640609d);
            Assert.assertEquals(fields.get("usage_guest_nice"), 58.79378775101397d);

            return null;
        };

        validateSingleRow(node, session, assertion);
    }

    private void validateSingleRow(final ObjectNode node, final ConnectorSession session,
                                   final Function<Block[], Void> assertionBlock)
            throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        MantaDataFileType dataFileType = MantaDataFileType.TELEGRAF_NDJSON;
        MantaLogicalTable table = new MantaLogicalTable("singlerow",
                testPathPrefix, dataFileType);
        {
            String tablePath = testPathPrefix + MantaLogicalTableProvider.TABLE_DEFINITION_FILENAME;
            MantaHttpHeaders headers = new MantaHttpHeaders().setContentType("application/json");
            byte[] data = mapper.writeValueAsBytes(table);

            mantaClient.put(tablePath, data, headers);
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
