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
import com.facebook.presto.spi.type.*;
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
import java.io.OutputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.joyent.manta.presto.record.telegraf.MantaTelegrafColumnLister.STRING_MAP;
import static com.joyent.manta.presto.test.MantaPrestoIntegrationTestUtils.setupConfiguration;

@Test
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

    public void canParseMapValues() throws IOException {
        final ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("timestamp", 1496275200);

        {
            final ObjectNode tags = new ObjectNode(JsonNodeFactory.instance);
            tags.put("arch", "x64");
            tags.put("datacenter", "ap-southeast-1b");
            node.put("tags", tags);
        }

        node.put("name", "cpu");

        {
            final ObjectNode fields = new ObjectNode(JsonNodeFactory.instance);
            fields.put("usage_guest", 94.59371357640609);
            fields.put("usage_guest_nice", 58.79378775101397);
            node.put("fields", fields);
        }

        final Function<Block[], Void> assertion = blocks -> {
            Object timestamp = TimestampType.TIMESTAMP.getObjectValue(session, blocks[0], 0);
            Object tags = STRING_MAP.getObjectValue(session, blocks[1], 0);
            String name = VarcharType.VARCHAR.getSlice(blocks[2], 0).toStringUtf8();
            Object fields = STRING_MAP.getObjectValue(session, blocks[3], 0);

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
