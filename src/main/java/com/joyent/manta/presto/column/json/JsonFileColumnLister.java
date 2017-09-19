/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column.json;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.presto.MantaPrestoFileType;
import com.joyent.manta.presto.column.ColumnLister;
import com.joyent.manta.presto.column.MantaPrestoColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class JsonFileColumnLister implements ColumnLister {
    private final ObjectMapper mapper;

    @Inject
    public JsonFileColumnLister(final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public List<ColumnMetadata> listColumns(final String objectPath,
                                            final MantaPrestoFileType fileType,
                                            final String firstLine) {
        final JsonNode node;

        try {
            node = mapper.readValue(firstLine, JsonNode.class);
        } catch (IOException e) {
            String msg = "Error parsing first line of new line NDJson file";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.addContextValue("firstLine", firstLine);
            me.addContextValue("objectPath", objectPath);
            throw me;
        }

        if (!(node instanceof ObjectNode)) {
            String msg = "JSON line should always be an object so that it can be "
                    + "converted to a columnar format";
            MantaPrestoRuntimeException me = new MantaPrestoRuntimeException(msg);
            me.addContextValue("nodeType", node.getNodeType());
            me.addContextValue("nodeClass", node.getClass());
            throw me;
        }

        final ObjectNode objectNode = (ObjectNode)node;

        ImmutableList.Builder<ColumnMetadata> columns = new ImmutableList.Builder<>();

        Iterator<Map.Entry<String, JsonNode>> itr = objectNode.fields();

        while (itr.hasNext()) {
            final Map.Entry<String, JsonNode> next = itr.next();
            String key = next.getKey();
            JsonNode val = next.getValue();

            Type type = null;
            String extraInfo = null;

            switch (val.getNodeType()) {
                case ARRAY:
                    type = VarcharType.VARCHAR;
                    extraInfo = "array";
                    break;
                case BINARY:
                    type = VarbinaryType.VARBINARY;
                    extraInfo = "binary";
                    break;
                case BOOLEAN:
                    type = BooleanType.BOOLEAN;
                    extraInfo = "boolean";
                    break;
                case MISSING:
                    type = VarcharType.VARCHAR;
                    extraInfo = "missing";
                    break;
                case NULL:
                    type = VarcharType.VARCHAR;
                    extraInfo = "null";
                    break;
                case NUMBER:
                    if (val.isBigDecimal()) {
                        type = DecimalType.createDecimalType();
                    } else if (val.isBigInteger()) {
                        type = BigintType.BIGINT;
                    } else if (val.isInt()) {
                        type = IntegerType.INTEGER;
                    } else if (val.isFloat() || val.isDouble()) {
                        type = DoubleType.DOUBLE;
                    } else {
                        type = DecimalType.createDecimalType();
                    }

                    extraInfo = "number";
                    break;
                case OBJECT:
                    type = VarcharType.VARCHAR;
                    extraInfo = "object";
                    break;
                case POJO:
                    type = VarcharType.VARCHAR;
                    extraInfo = "pojo";
                    break;
                case STRING:
                    type = VarcharType.VARCHAR;
                    extraInfo = "string";
                    break;
            }

            if (type != null) {
                columns.add(new MantaPrestoColumn(key, type, extraInfo));
            }
        }

        return columns.build();
    }
}
