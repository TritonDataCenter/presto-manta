/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.json;

import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.TimestampType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.MantaCountingInputStream;
import com.joyent.manta.presto.column.MantaColumn;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
public class MantaJsonRecordCursorTest {

    // TIMESTAMP PARSING

    public void canParseEpochMillisecondsAsTimestamp() {
        final MantaColumn column = new MantaColumn("timestamp", TimestampType.TIMESTAMP,
                null, "[timestamp] epoch-milliseconds", false, null);
        final long expected = 1521226897201L; // 2018-03-16T19:01:37.201Z
        final JsonNode node = new LongNode(expected);
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getTimestamp(node, column);
        Assert.assertEquals(actual, expected);
    }

    public void canParseEpochSecondsAsTimestamp() {
        final MantaColumn column = new MantaColumn("timestamp", TimestampType.TIMESTAMP,
                null, "[timestamp] epoch-seconds", false, null);
        final long epochSeconds = 1521226897; // 2018-03-16T19:01:37Z
        final long expected = 1521226897000L;

        final JsonNode node = new LongNode(epochSeconds);
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getTimestamp(node, column);
        Assert.assertEquals(actual, expected);
    }

    public void canParseEpochDaysAsTimestamp() {
        final MantaColumn column = new MantaColumn("timestamp", TimestampType.TIMESTAMP,
                null, "[timestamp] epoch-days", false, null);
        final long epochDays = 17606; // 2018-03-16T00:00:00Z
        final long expected = 1521158400000L;

        final JsonNode node = new LongNode(epochDays);
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getTimestamp(node, column);
        Assert.assertEquals(actual, expected);
    }

    public void canParseDefaultLongValueAsEpochMillisecondsTimestamp() {
        final MantaColumn column = new MantaColumn("timestamp", TimestampType.TIMESTAMP,
                null);
        final long expected = 1521226897201L; // 2018-03-16T19:01:37.201Z
        final JsonNode node = new LongNode(expected);
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getTimestamp(node, column);
        Assert.assertEquals(actual, expected);
    }

    public void canParseIso8601AsTimestamp() {
        final MantaColumn column = new MantaColumn("timestamp", TimestampType.TIMESTAMP,
                null, "[timestamp] iso-8601", false, null);
        final Instant instant = Instant.parse("2018-03-16T19:01:37.201Z");
        final long expected = 1521226897201L; // 2018-03-16T19:01:37.201Z

        final JsonNode node = new TextNode(instant.toString());
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getTimestamp(node, column);
        Assert.assertEquals(actual, expected);
    }

    public void canParseHTTPTimestampAsTimestamp() {
        final MantaColumn column = new MantaColumn("timestamp", TimestampType.TIMESTAMP,
                null, "[timestamp] EEE, dd MMM yyyy HH:mm:ss zzz", false, null);
        final String timestamp = "Fri, 16 Mar 2018 19:01:37 GMT";
        final long expected = 1521226897000L; // 2018-03-16T19:01:37Z

        final JsonNode node = new TextNode(timestamp);
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getTimestamp(node, column);
        Assert.assertEquals(actual, expected);
    }

    // DATE PARSING

    public void canParseEpochMillisecondsAsDate() {
        final MantaColumn column = new MantaColumn("date", DateType.DATE,
                null, "[date] epoch-milliseconds", false, null);
        final long epochMillisDate = 1521226897201L; // 2018-03-16T19:01:37.201Z
        final long expected = LocalDate.parse("2018-03-16",
                DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochDay();
        final JsonNode node = new LongNode(epochMillisDate);
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getDate(node, column);
        Assert.assertEquals(actual, expected);
    }

    public void canParseEpochDaysAsDate() {
        final MantaColumn column = new MantaColumn("date", DateType.DATE,
                null, "[date] epoch-days", false, null);
        final long expected = 17606; // 2018-03-16
        final JsonNode node = new LongNode(expected);
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getDate(node, column);
        Assert.assertEquals(actual, expected);
    }

    public void canParseDefaultLongValueAsEpochDaysDate() {
        final MantaColumn column = new MantaColumn("date", DateType.DATE,
                null);
        final long expected = 17606; // 2018-03-16
        final JsonNode node = new LongNode(expected);
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getDate(node, column);
        Assert.assertEquals(actual, expected);
    }

    public void canParseIso8601AsDate() {
        final MantaColumn column = new MantaColumn("date", DateType.DATE,
                null, "[date] iso-8601", false, null);
        final Instant instant = Instant.parse("2018-03-16T19:01:37.201Z");
        final long expected = 17606; // 2018-03-16T00:00:00Z

        final JsonNode node = new TextNode(instant.toString());
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getDate(node, column);
        Assert.assertEquals(actual, expected);
    }

    public void canParseYYYYMMDDAsDate() {
        final MantaColumn column = new MantaColumn("date", DateType.DATE,
                null, "[date] yyyy-MM-dd", false, null);
        final String date = "2018-03-16";
        final long expected = 17606; // 2018-03-16T00:00:00Z

        final JsonNode node = new TextNode(date);
        final MantaJsonRecordCursor instance = mockInstance(ImmutableList.of(column));

        final long actual = instance.getDate(node, column);
        Assert.assertEquals(actual, expected);
    }

    // UTILITY METHODS

    private MantaJsonRecordCursor mockInstance(List<MantaColumn> columns) {
        MantaJsonDataFileObjectMapperProvider mapperProvider = new MantaJsonDataFileObjectMapperProvider();
        ObjectMapper mapper = mapperProvider.get();

        MantaObject object = mock(MantaObject.class);
        when(object.getPath()).thenReturn("/user/stor/file.json");
        InputStream inputStream = new ByteArrayInputStream("{}".getBytes(StandardCharsets.UTF_8));
        ObjectReader streamingReader = mapper.readerFor(ObjectNode.class);
        return new MantaJsonRecordCursor(null, columns,
                "", 0L, new MantaCountingInputStream(inputStream, object),
                streamingReader);
    }
}
