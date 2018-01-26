/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.json;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.JsonType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.MantaConnectorId;
import com.joyent.manta.presto.column.AbstractPeekingColumnLister;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoRuntimeException;
import com.joyent.manta.presto.exceptions.MantaPrestoUncheckedIOException;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * {@link com.joyent.manta.presto.column.ColumnLister} implementation that
 * creates a column list based on the first line of JSON read from a new line
 * delimited JSON file.
 *
 * @since 1.0.0
 */
public class MantaJsonFileColumnLister extends AbstractPeekingColumnLister {
    private static final int CACHE_CONCURRENCY_LEVEL = 20;
    private static final int MAX_CACHE_SIZE = 2000;

    private static final String[] DATE_KEYWORDS = new String[] {
            "date", "timestamp"
    };

    /**
     * List of parseable date formats.
     */
    public static final Map<String, String> DATE_FORMAT_REGEXPS =
            new ImmutableMap.Builder<String, String>()
                    .put("^\\d{8}$", "yyyyMMdd")
                    .put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy")
                    .put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-MM-dd")
                    .put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "MM/dd/yyyy")
                    .put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/MM/dd")
                    .put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy")
                    .put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy")
                    .build();

    private final ObjectMapper jsonDataFileMapper;

    /**
     * Cache containing the column list per query, so that when we look up
     * columns, we only have to do it once per query run.
     */
    private final Cache<String, List<MantaColumn>> columnCache =
            CacheBuilder.newBuilder()
                    .concurrencyLevel(CACHE_CONCURRENCY_LEVEL)
                    .maximumSize(MAX_CACHE_SIZE)
                    .build();

    /**
     * {@link Callable} implementation that loads the columns from the first
     * line of JSON from the first file found.
     */
    private final class ColumnListLoader implements Callable<List<MantaColumn>> {
        private final MantaSchemaTableName tableName;
        private final MantaLogicalTable table;

        private final Logger log = LoggerFactory.getLogger(ColumnListLoader.class);

        private ColumnListLoader(final MantaSchemaTableName tableName,
                                 final MantaLogicalTable table) {
            this.tableName = tableName;
            this.table = table;
        }

        @Override
        public List<MantaColumn> call() {
            final ImmutableList.Builder<MantaColumn> columns = new ImmutableList.Builder<>();
            Optional<JsonNode> colCfg = table.getColumnConfig();
            if (colCfg.isPresent()) {
                log.debug("In colCfg is Present");

                final Iterator<JsonNode> colCfgItr = colCfg.get().elements();
                while (colCfgItr.hasNext()) {
                    JsonNode colCfgEnt = colCfgItr.next();
                    String colname  = colCfgEnt.findValue("column").textValue();
                    String coltype =  colCfgEnt.findValue("type").textValue();

                    if (log.isDebugEnabled()
                            && (StringUtils.isEmpty(colname) || StringUtils.isEmpty(coltype))) {
                        log.debug("coltype or colname null");
                    }

                    MantaColumn column = buildColumnFromNameAndType(colname, coltype);

                    if (column != null) {
                        columns.add(column);
                    }
                }
            } else {
                log.debug("In colCfg is NOT Present");


                final MantaObject first = firstObjectForTable(tableName, table);
                final String objectPath = first.getPath();
                final String firstLine = readFirstLine(objectPath);


                final ObjectNode objectNode = readObjectNode(firstLine, objectPath);
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
            }
            return columns.build();
        }
    }

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param connectorId presto connection id object for debugging
     * @param mantaClient object that allows for direct operations on Manta
     * @param maxBytesPerLine number of bytes from the start of file to request
     *                        via a range request so we don't have to download
     *                        the entire file
     * @param jsonDataFileMapper Jackson JSON serialization / deserialization object
     */
    @Inject
    public MantaJsonFileColumnLister(final MantaConnectorId connectorId,
                                     final MantaClient mantaClient,
                                     @Named("MaxBytesPerLine") final Integer maxBytesPerLine,
                                     @Named("JsonData") final ObjectMapper jsonDataFileMapper) {
        super(connectorId, mantaClient, maxBytesPerLine);
        this.jsonDataFileMapper = jsonDataFileMapper;
    }

    @Override
    public List<MantaColumn> listColumns(final MantaSchemaTableName tableName,
                                         final MantaLogicalTable table,
                                         final ConnectorSession session) {
        final ColumnListLoader loader = new ColumnListLoader(tableName, table);
        try {

            return columnCache.get(session.getQueryId(), loader);
        } catch (ExecutionException e) {
            String msg = "Error loading column listing from JSON source";
            MantaPrestoRuntimeException mpre = new MantaPrestoRuntimeException(msg);
            mpre.setContextValue("tableName", tableName.getTableName());
            mpre.setContextValue("schema", tableName.getSchemaName());

            throw mpre;
        }
    }

    private ObjectNode readObjectNode(final String firstLine, final String objectPath) {
        final JsonNode node;

        try {
            node = jsonDataFileMapper.readValue(firstLine, JsonNode.class);
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
                final String dateFormat = findDateFormat(key, val);
                if (dateFormat != null) {
                    type = DateType.DATE;
                    extraInfo = "date " + dateFormat;
                } else {
                    type = VarcharType.VARCHAR;
                    extraInfo = "string";
                }

                break;
            default:
                return null;
        }

        return new MantaColumn(key, type, extraInfo);
    }

    private MantaColumn buildColumnFromNameAndType(final String key, final String inType) {
        final Type type;
        final String extraInfo;

        switch (inType) {
            case "bool":
                type = BooleanType.BOOLEAN;
                extraInfo = "boolean";
                break;
            case "null":
                type = VarcharType.VARCHAR;
                extraInfo = "null";
                break;
            case "int":
                type = IntegerType.INTEGER;
                extraInfo = "number";
                break;
            case "json":
                type = JsonType.JSON;
                extraInfo = "jsonObject";
                break;
            case "string":
                type = VarcharType.VARCHAR;
                extraInfo = "string";
                break;
            default:
                return null;
        }

        return new MantaColumn(key, type, extraInfo);
    }

    private String findDateFormat(final String key, final JsonNode val) {
        final String text = val.asText();
        for (String keyword : DATE_KEYWORDS) {
            if (StringUtils.containsIgnoreCase(key, keyword)) {
                for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
                    if (text.toLowerCase().matches(regexp)) {
                        return DATE_FORMAT_REGEXPS.get(regexp);
                    }
                }
            }
        }

        return null;
    }

    private Type findNumericType(final JsonNode val) {
        final Type type;

        // We don't process BigDecimals because of: https://github.com/prestodb/presto/issues/9103

        if (val.isBigInteger()) {
            type = BigintType.BIGINT;
        } else if (val.isInt() || val.isLong()) {
            type = BigintType.BIGINT;
        } else if (val.isFloat() || val.isDouble()) {
            type = DoubleType.DOUBLE;
        } else {
            type = DecimalType.createDecimalType();
        }

        return type;
    }
}
