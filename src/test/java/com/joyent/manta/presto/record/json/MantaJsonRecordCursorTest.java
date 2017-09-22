/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.json;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingInputStream;
import com.google.inject.Injector;
import com.joyent.manta.presto.MantaPrestoTestUtils;
import com.joyent.manta.presto.column.MantaColumn;
import io.airlift.slice.Slice;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.JsonType.JSON;

@Test
public class MantaJsonRecordCursorTest {
    private static final String TEST_FILE = "test-data/sample-data.ndjson.gz";
    private static final boolean OUTPUT_ENABLED = false;

    private Injector injector;
    private ObjectMapper objectMapper;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
        objectMapper = injector.getInstance(ObjectMapper.class);
    }

    @SuppressWarnings("Duplicates")
    public void canParseJsonSampleRecordsWithoutAnError() throws IOException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        List<MantaColumn> columns = buildColumnList();

        final long totalBytes = 37782293;

        InputStream in = classLoader.getResourceAsStream(TEST_FILE);
        GZIPInputStream gzIn = new GZIPInputStream(in);
        CountingInputStream cin = new CountingInputStream(gzIn);

        try (MantaJsonRecordCursor cursor = new MantaJsonRecordCursor(
                columns, "/user/fake/object", totalBytes, cin, objectMapper)) {

            final int columnLen = columns.size();
            long line = 0L;
            while (cursor.advanceNextPosition()) {
                print("[");
                printf("%06d", ++line);

                for (int i = 0; i < columnLen; i++) {
                    printf(" col-%d=", i);

                    if (cursor.isNull(i)) {
                        print("null");
                        continue;
                    }

                    Type type = cursor.getType(i);
                    Class<?> javaType = type.getJavaType();
                    if (javaType == boolean.class) {
                        final boolean val = cursor.getBoolean(i);

                        print("boolean");
                        printf(" (%b)", val);
                    } else if (javaType == long.class) {
                        final long val = cursor.getLong(i);

                        print("long");
                        printf(" (%d)", val);
                    } else if (javaType == double.class) {
                        final double val = cursor.getDouble(i);

                        print("double");
                        printf(" (%d)", val);
                    } else if (javaType == Slice.class) {
                        final String val = cursor.getSlice(i).toStringUtf8();
                        Assert.assertNotNull(val);

                        print("slice");
                        printf(" (%s)", val);
                    } else {
                        final String val = cursor.getObject(i).toString();
                        Assert.assertNotNull(val);

                        print("object");
                        printf(" (%s)", val);
                    }
                }

                println("]");
            }

            Duration readTime = Duration.of(cursor.getReadTimeNanos(), ChronoUnit.NANOS);
            System.err.printf("Read file in %d ms\n", readTime.toMillis());
        }
    }

    private void print(final String s) {
        if (OUTPUT_ENABLED) {
            System.out.print(s);
        }
    }

    private void printf(final String s, Object... args) {
        if (OUTPUT_ENABLED) {
            System.out.printf(s, args);
        }
    }

    private void println(final String s) {
        if (OUTPUT_ENABLED) {
            System.out.println(s);
        }
    }

    private List<MantaColumn> buildColumnList() {
        ImmutableList.Builder<MantaColumn> columns =
                new ImmutableList.Builder<>();

        columns.add(new MantaColumn("name", VARCHAR, "string"));
        columns.add(new MantaColumn("article_id", VARCHAR, "string"));
        columns.add(new MantaColumn("publisher_id", VARCHAR, "string"));
        columns.add(new MantaColumn("tracking_id", VARCHAR, "string"));
        columns.add(new MantaColumn("count", INTEGER, "number"));
        columns.add(new MantaColumn("resolution", VARCHAR, "string"));
        columns.add(new MantaColumn("ad_unit", VARCHAR, "string"));
        columns.add(new MantaColumn("properties", JSON, "jsonObject"));
        columns.add(new MantaColumn("timestamp", INTEGER, "number"));

        return columns.build();
    }
}
