/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.MantaPrestoUtils;
import com.joyent.manta.presto.record.json.MantaJsonFileColumnLister;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Jackson deserializer class used to deserialize the
 * <code>presto-tables.json</code> table definition file direct from Manta.
 * This deserializer is more forgiving the annotations default implementation
 * and it provides more actionable error messages back to the user.
 *
 * Even though the {@link com.fasterxml.jackson.databind.ObjectMapper} instance
 * that we use within the Manta Presto Connector is configured to use this
 * deserializer the {@link com.fasterxml.jackson.databind.ObjectMapper} instance
 * that Presto itself uses is not configured to use this and it falls back on
 * the annotations in {@link MantaLogicalTable}.
 *
 * @since 1.0.0
 */
public class MantaLogicalTableDeserializer extends JsonDeserializer<MantaLogicalTable> {
    private final ConfigContext config;
    public final static Logger LOG = LoggerFactory.getLogger(MantaLogicalTableDeserializer.class);
    /**
     * Creates a new instance base on the instantiated Manta configuration.
     *
     * @param config configuration context so we can get the Manta home directory
     */
    @Inject
    public MantaLogicalTableDeserializer(final ConfigContext config) {
        this.config = config;
    }

    @Override
    public MantaLogicalTable deserialize(final JsonParser p,
                                         final DeserializationContext ctxt)
            throws IOException {
        final ObjectCodec codec = p.getCodec();
        final JsonNode node = codec.readTree(p);

        if (!node.isObject()) {
            throw new JsonMappingException(p, "Expected JSON source to be an "
                    + "object when parsing for a MantaLogicalTable object");
        }

        @SuppressWarnings("unchecked")
        final ObjectNode objectNode = (ObjectNode)node;

        final String name = readRequiredString(objectNode.get("name"), "name", p);

        final String rootPath = MantaPrestoUtils.substitudeHomeDirectory(
                readRequiredString(objectNode.get("rootPath"), "rootPath", p),
                config.getMantaHomeDirectory());

        final String dataFileTypeValue = readRequiredString(objectNode.get("dataFileType"), "dataFileType", p);
        final MantaDataFileType dataFileType;

        try {
            dataFileType = MantaDataFileType.valueOf(dataFileTypeValue);
        } catch (IllegalArgumentException e) {
            String msg = String.format("Unsupported data file type specified [%s]",
                    dataFileTypeValue);
            throw new JsonMappingException(p, msg, e);
        }

        final Optional<MantaLogicalTablePartitionDefinition> partitionDefinition =
                readPartitionDefinition(objectNode, p);
        LOG.debug("before readColumnsArray");
        final Optional<JsonNode> columnConfig = readColumnsArray(objectNode.get("columnConfig"), p);

        try {
            return new MantaLogicalTable(name, rootPath, dataFileType,
                    partitionDefinition, columnConfig);
        } catch (Exception e) {
            throw new JsonMappingException(p, "Unable to create new "
                    + "MantaLogicalTable instance", e);
        }
    }

    /**
     * Reads the partition section of a table logical definition object.
     */
    private static Optional<MantaLogicalTablePartitionDefinition> readPartitionDefinition(
            final ObjectNode objectNode, final JsonParser p) throws JsonProcessingException {
        if (objectNode.get("partitioning") != null && !objectNode.get("partitioning").isNull()) {
            if (!objectNode.get("partitioning").isObject()) {
                throw new JsonMappingException(p, "Expected partition value to "
                        + "be a JSON object");
            }

            @SuppressWarnings("unchecked")
            final ObjectNode partitioning = (ObjectNode)objectNode.get("partitioning");

            final Pattern directoryFilterRegex = readPattern(
                    partitioning.get("directoryFilterRegex"), "directoryFilterRegex",
                    p);

            final Pattern filterRegex = readPattern(partitioning.get("filterRegex"),
                    "filterRegex", p);

            final LinkedHashSet<String> directoryFilterPartitions =
                    readOrderedSet(partitioning.get("directoryPartitions"),
                            "directoryPartitions", p);
            final LinkedHashSet<String> filterPartitions =
                    readOrderedSet(partitioning.get("partitions"), "segments", p);

            /* If all values are empty/default, then it makes sense to return
             * back an empty Optional.empty() because functionally there is
             * no partition specified. */
            if (directoryFilterRegex == null && filterRegex == null
                    && directoryFilterPartitions.isEmpty() && filterPartitions.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(new MantaLogicalTablePartitionDefinition(
                        directoryFilterRegex, filterRegex, directoryFilterPartitions,
                        filterPartitions));
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * Attempts to read a string that must be present. If it can't, it errors.
     */
    private static String readRequiredString(final JsonNode node, final String fieldName,
                                             final JsonParser p) throws JsonProcessingException {
        if (node == null || node.isNull()) {
            String msg = String.format("Expected JSON source to have "
                    + "[%s] defined when parsing for a MantaLogicalTable object",
                    fieldName);
            throw new JsonMappingException(p, msg);
        }

        if (!node.isTextual()) {
            String msg = String.format("Expected JSON source to have "
                    + "[%s] defined as a textual element when parsing for "
                    + "a MantaLogicalTable object", fieldName);
            throw new JsonMappingException(p, msg);
        }

        return node.asText();
    }

    /**
     * Attempts to read a regex pattern as a String and then compile it. If it
     * can't it errors.
     */
    private static Pattern readPattern(final JsonNode node, final String fieldName,
                                       final JsonParser p) throws JsonProcessingException {
        final Pattern regex;

        if (node == null || node.isNull()) {
            regex = null;
        } else if (node.isTextual()) {
            String filterRegexValue = node.asText();

            if (StringUtils.isBlank(filterRegexValue)) {
                regex = null;
            } else {
                try {
                    regex = Pattern.compile(filterRegexValue);
                } catch (IllegalArgumentException e) {
                    String msg = String.format("Bad regular expressed "
                            + "passed as filter [%s]", fieldName);
                    throw new JsonMappingException(p, msg, e);
                }
            }
        } else {
            String msg = String.format("Expected JSON source to have a "
                    + "%s defined as a textual element when parsing for "
                    + "a MantaLogicalTable object", fieldName);
            throw new JsonMappingException(p, msg);
        }

        return regex;
    }


    /**
     * Attempts to read a string array from JSON as a {@link LinkedHashSet}.
     */
    private static LinkedHashSet<String> readOrderedSet(final JsonNode node,
            final String fieldName, final JsonParser p) throws JsonProcessingException {
        final LinkedHashSet<String> set = new LinkedHashSet<>();

        if (node == null || node.isNull()) {
            return set;
        }

        if (!node.isArray()) {
            String msg = String.format("Expected JSON source to have a "
                    + "%s defined as an array element when parsing for "
                    + "a MantaLogicalTable object", fieldName);
            throw new JsonMappingException(p, msg);
        }

        @SuppressWarnings("unchecked") final ArrayNode array = (ArrayNode) node;

        for (JsonNode value : array) {
            if (!value.isTextual()) {
                String msg = String.format("Expected JSON source to have a "
                        + "%s defined as an array element with only textual "
                        + "values when parsing for "
                        + "a MantaLogicalTable object", fieldName);
                throw new JsonMappingException(p, msg);
            }

            set.add(value.textValue());
        }

        return set;
    }

    /**
     * Reads & Verifies column JsonNode format, returns null if columnConfig is not an arrays.
     */

    private static Optional<JsonNode> readColumnsArray(final JsonNode columnConfig,
                                             final JsonParser p) throws JsonProcessingException {
        final Optional<JsonNode> retnode;
        LOG.debug("readColumnsArray");
        /**
         *  If this 'columns' field is not present in the .json, or empty just return null
         *  We'll detect this and use the first line of the .json to define the columns instead.
         */
        if ((columnConfig != null) && columnConfig.isArray()) {
            // Check whether each array element has the required columns
            for (JsonNode element : columnConfig) {
                if (element.isObject()) {/*
                    if (!(element.has("column")
                            && element.has("displayname") && element.has("type"))) {
                        String msg = String.format("Expected JSON columns Array to have elements "
                                + "column, display_name, and type defined for each object in array.");
                        throw new JsonMappingException(p, msg);
                    }*/
                } else {
                    String msg = String.format("Expected JSON columns Array element"
                            + "to be a json object.");
                    throw new JsonMappingException(p, msg);
                }
            }
            retnode = Optional.of(columnConfig);
        } else {
            retnode = Optional.empty();
        }
        return retnode;

    }
}
