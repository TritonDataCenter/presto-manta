/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.json;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.JsonType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.MantaConnectorId;
import com.joyent.manta.presto.column.AbstractPeekingColumnLister;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {@link com.joyent.manta.presto.column.ColumnLister} implementation that
 * creates a column list based on the first line of JSON read from a new line
 * delimited JSON file.
 *
 * @since 1.0.0
 */
public class MantaJsonFileColumnLister extends AbstractPeekingColumnLister {
    private final ObjectMapper mapper;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param connectorId presto connection id object for debugging
     * @param mantaClient object that allows for direct operations on Manta
     * @param maxBytesPerLine number of bytes from the start of file to request
     *                        via a range request so we don't have to download
     *                        the entire file
     * @param mapper Jackson JSON serialization / deserialization object
     */
    @Inject
    public MantaJsonFileColumnLister(final MantaConnectorId connectorId,
                                     final MantaClient mantaClient,
                                     @Named("MaxBytesPerLine") final Integer maxBytesPerLine,
                                     final ObjectMapper mapper) {
        super(connectorId, mantaClient, maxBytesPerLine);
        this.mapper = mapper;
    }

    @Override
    public List<MantaColumn> listColumns(final MantaSchemaTableName tableName,
                                         final MantaLogicalTable table) {
        final MantaObject first = firstObjectForTable(tableName, table);
        final String objectPath = first.getPath();
        final String firstLine = readFirstLine(objectPath);

        final ObjectNode objectNode = readObjectNode(firstLine, objectPath);
        final ImmutableList.Builder<MantaColumn> columns = new ImmutableList.Builder<>();
        final Iterator<Map.Entry<String, JsonNode>> itr = objectNode.fields();

        while (itr.hasNext()) {
            final Map.Entry<String, JsonNode> next = itr.next();
            String key = next.getKey();
            JsonNode val = next.getValue();

            MantaColumn column = buildColumn(key, val);

            if (column != null) {
                columns.add(column);
            }
        }

        return columns.build();
    }

    private ObjectNode readObjectNode(final String firstLine, final String objectPath) {
        final JsonNode node;

        try {
            node = mapper.readValue(firstLine, JsonNode.class);
        } catch (IOException e) {
            String msg = "Error parsing first line of new line NDJson file";
            MantaPrestoUncheckedIOException me = new MantaPrestoUncheckedIOException(msg, e);
            me.setContextValue("connectorId", getConnectorId());
            me.addContextValue("firstLine", firstLine);
            me.addContextValue("objectPath", objectPath);
            throw me;
        }

        if (!(node instanceof ObjectNode)) {
            String msg = "JSON line should always be an object so that it can be "
                    + "converted to a columnar format";
            MantaPrestoRuntimeException me = new MantaPrestoRuntimeException(msg);
            me.setContextValue("connectorId", getConnectorId());
            me.addContextValue("nodeType", node.getNodeType());
            me.addContextValue("nodeClass", node.getClass());
            throw me;
        }

        return (ObjectNode)node;
    }

    private MantaColumn buildColumn(final String key, final JsonNode val) {
        final Type type;
        final String extraInfo;

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
                type = findNumericType(val);
                extraInfo = "number";
                break;
            case OBJECT:
                type = JsonType.JSON;
                extraInfo = "jsonObject";
                break;
            case POJO:
                type = VarcharType.VARCHAR;
                extraInfo = "pojo";
                break;
            case STRING:
                type = VarcharType.VARCHAR;
                extraInfo = "string";
                break;
            default:
                return null;
        }

        return new MantaColumn(key, type, extraInfo);
    }

    private Type findNumericType(final JsonNode val) {
        final Type type;

        if (val.isBigDecimal()) {
            type = DecimalType.createDecimalType();
        } else if (val.isBigInteger()) {
            type = BigintType.BIGINT;
        } else if (val.isInt()) {
            type = BigintType.BIGINT;
        } else if (val.isFloat() || val.isDouble()) {
            type = DoubleType.DOUBLE;
        } else {
            type = DecimalType.createDecimalType();
        }

        return type;
    }
}
