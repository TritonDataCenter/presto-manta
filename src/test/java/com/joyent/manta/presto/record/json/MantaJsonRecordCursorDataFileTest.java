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
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.MantaCountingInputStream;
import com.joyent.manta.presto.column.MantaColumn;
import io.airlift.slice.Slice;
import org.junit.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.JsonType.JSON;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test takes an approach of using data files to load in records and
 * to test the functionality of a record cursor.
 */
@Test
public class MantaJsonRecordCursorDataFileTest {
    private static final boolean OUTPUT_ENABLED = false;

    public void canParseJsonGZSampleRecordsWithoutAnError() throws IOException {
        canParseJsonSampleRecordsWithoutAnError("test-data/cursor/sample-data.ndjson.gz");
    }

    public void canParseJsonHadoopSnappySampleRecordsWithoutAnError() throws IOException {
        try {
            canParseJsonSampleRecordsWithoutAnError("test-data/cursor/sample-data.ndjson.snappy");
        } catch (UnsupportedOperationException e) {
            throw new SkipException("Native libraries need to be loaded in "
                    + "order to use decompression algorithm");
        }
    }

    public void canParseJsonXZSampleRecordsWithoutAnError() throws IOException {
        canParseJsonSampleRecordsWithoutAnError("test-data/cursor/sample-data.ndjson.xz");
    }

    public void canParseJsonBzip2SampleRecordsWithoutAnError() throws IOException {
        canParseJsonSampleRecordsWithoutAnError("test-data/cursor/sample-data.ndjson.bz2");
    }

    @SuppressWarnings("Duplicates")
    private void canParseJsonSampleRecordsWithoutAnError(final String testFile) throws IOException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        List<MantaColumn> columns = buildColumnList();

        final long totalBytes = 37782293;

        InputStream in = classLoader.getResourceAsStream(testFile);
        MantaObject object = mock(MantaObject.class);
        when(object.getPath()).thenReturn(testFile);

        MantaCountingInputStream cin = new MantaCountingInputStream(in, object);
        MantaJsonDataFileObjectMapperProvider mapperProvider = new MantaJsonDataFileObjectMapperProvider();
        ObjectMapper mapper = mapperProvider.get();
        ObjectReader streamingReader = mapper.readerFor(ObjectNode.class);

        try (MantaJsonRecordCursor cursor = new MantaJsonRecordCursor(null,
                columns, cin.getPath(), totalBytes, cin,
                streamingReader)) {

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
            System.err.printf("Read file [%s] in %d ms\n", testFile, readTime.toMillis());
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

        columns.add(new MantaColumn("date", DATE, "date string",
                "[date] yyyy-MM-dd", false, null));
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
