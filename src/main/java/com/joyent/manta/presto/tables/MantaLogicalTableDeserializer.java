/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.MantaPrestoUtils;
import com.joyent.manta.presto.column.MantaColumn;
import com.joyent.manta.presto.types.TypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
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
    /**
     * Manta configuration object.
     */
    private final ConfigContext config;

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaLogicalTableDeserializer.class);

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
    @Nonnull
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

        final MantaLogicalTablePartitionDefinition partitionDefinition =
                readPartitionDefinition(objectNode, p);

        final List<MantaColumn> columnConfig = readColumnsArray(objectNode.get("columns"), p);

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
    private static MantaLogicalTablePartitionDefinition readPartitionDefinition(
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
            final LinkedHashSet<String> fileFilterPartitions =
                    readOrderedSet(partitioning.get("partitions"), "segments", p);

            final Sets.SetView<String> partitionNameDuplicates =
                    Sets.intersection(directoryFilterPartitions, fileFilterPartitions);

            if (!partitionNameDuplicates.isEmpty()) {
                final String duplicates = Joiner.on(", ").join(partitionNameDuplicates);

                String msg = String.format("Duplicate names found between "
                                + "directoryPartitions and partitions. Each partition "
                                + "name must be unique. Duplicates: %s",
                        duplicates);
                throw new JsonMappingException(p, msg);
            }

            /* If all values are empty/default, then it makes sense to return
             * back an empty Optional.empty() because functionally there is
             * no partition specified. */
            if (directoryFilterRegex == null && filterRegex == null
                    && directoryFilterPartitions.isEmpty() && fileFilterPartitions.isEmpty()) {
                return null;
            } else {
                return new MantaLogicalTablePartitionDefinition(
                        directoryFilterRegex, filterRegex, directoryFilterPartitions,
                        fileFilterPartitions);
            }
        } else {
            return null;
        }
    }

    /**
     * Attempts to read a string that must be present. If it can't, it errors.
     *
     * @throws JsonMappingException thrown when the JSON file contains invalid values
     */
    @Nonnull
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
     *
     * @throws JsonMappingException thrown when the JSON file contains invalid values
     */
    @Nullable
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
     * Reads & Verifies column JsonNode format, returns null if columnConfig
     * is not an arrays.
     *
     * @throws JsonMappingException thrown when the JSON file contains invalid values
     */
    @Nullable
    private static List<MantaColumn> readColumnsArray(
            final JsonNode columnConfig, final JsonParser p)
            throws JsonProcessingException {

        final List<MantaColumn> columnList;

        /*
         *  If this 'columns' field is not present in the .json, or empty just
         *  return null.
         *  We'll detect this and use the first line of the .json to define the
         *  columns instead.
         */
        if ((columnConfig != null) && columnConfig.isArray()) {
            final ImmutableList.Builder<MantaColumn> columnBuilder =
                    ImmutableList.builder();

            // Check whether each array element has the required columns
            for (JsonNode element : columnConfig) {
                if (!element.isObject()) {
                    String msg = String.format("Expected JSON columns array element"
                            + "to be a json object [actualType=%s].",
                            Objects.toString(element.getNodeType()));
                    throw new JsonMappingException(p, msg);
                }

                @SuppressWarnings("unchecked")
                final ObjectNode objectNode = (ObjectNode)element;

                final String name = readColumnName(objectNode, p);
                final Type type = readType(objectNode, p);
                final String extraInfo = readFormat(objectNode, p, type);

                MantaColumn column = new MantaColumn(
                        name, type, null, extraInfo, false);
                columnBuilder.add(column);
            }

            columnList = columnBuilder.build();
        } else {
            columnList = null;
        }

        return columnList;
    }

    /**
     * Reads the name of a column from a JSON element.
     *
     * @param element element to read from
     * @param p json parser to embed in error messages
     * @return the column name as a string
     *
     * @throws JsonMappingException thrown when the JSON file contains invalid values
     */
    private static String readColumnName(final ObjectNode element, final JsonParser p)
            throws JsonMappingException {
        final JsonNode name = element.get("name");

        if (name == null || name.isNull()) {
            String msg = "Expected JSON element [name] to not be null.";
            throw new JsonMappingException(p, msg);
        }

        if (!name.isTextual()) {
            String msg = "Expected JSON [name] element to be a string.";
            throw new JsonMappingException(p, msg);
        }

        final String text = name.asText();

        if (StringUtils.isBlank(text)) {
            String msg = "Expected JSON [name] element to not be blank.";
            throw new JsonMappingException(p, msg);
        }

        return text;
    }

    /**
     * Reads the type of a column from a JSON element.
     *
     * @param element element to read from
     * @param p json parser to embed in error messages
     * @return the Presto type as an object
     *
     * @throws JsonMappingException thrown when the JSON file contains invalid values
     */
    @Nonnull
    private static Type readType(final ObjectNode element, final JsonParser p)
            throws JsonMappingException {
        final JsonNode type = element.get("type");

        if (type == null || type.isNull()) {
            String msg = String.format("Expected JSON element [type] "
                    + "to not be null.");
            throw new JsonMappingException(p, msg);
        }

        if (!type.isTextual()) {
            String msg = String.format("Expected JSON element [type] "
                    + "to be a string.");
            throw new JsonMappingException(p, msg);
        }

        final String text = type.asText();

        if (StringUtils.isBlank(text)) {
            String msg = String.format("Expected JSON element [type] "
                    + "to not be blank.");
            throw new JsonMappingException(p, msg);
        }

        final Type typeDefinition = TypeUtils.parseTypeFromString(text);

        if (typeDefinition == null) {
            String msg = String.format("Unrecognized value for [type]: "
                    + "%s", text);
            throw new JsonMappingException(p, msg);
        }

        return typeDefinition;
    }

    /**
     * Reads the format of a column from a JSON element for use in the
     * extraInfo portion of a {@link MantaColumn}'s metadata.
     *
     * @param element element to read from
     * @param p json parser to embed in error messages
     * @param type presto type value in which the format will be applied
     * @return the format in which a type is being parsed
     *
     * @throws JsonMappingException thrown when the JSON file contains invalid values
     */
    @Nullable
    private static String readFormat(final ObjectNode element, final JsonParser p,
                                     final Type type)
            throws JsonMappingException {
        Objects.requireNonNull(type, "The type must not be null");

        final JsonNode format = element.get("format");

        if (format == null || format.isNull()) {
            return null;
        }

        if (!format.isTextual()) {
            String msg = "Expected JSON element [format] to be a string";
            throw new JsonMappingException(p, msg);
        }

        String formatText = format.asText();

        if (StringUtils.isBlank(formatText)) {
            return null;
        }

        final String afterTypeSignature = StringUtils.substringAfter(formatText,
                "[" + type.getTypeSignature() + "] ");

        if (type.equals(TimestampType.TIMESTAMP) || type.equals(DateType.DATE)) {
            final String pattern;

            if (StringUtils.isBlank(afterTypeSignature)) {
                pattern = formatText;
            } else {
                pattern = afterTypeSignature;
            }

            switch (pattern) {
                case "iso-8601":
                case "epoch-milliseconds":
                case "epoch-seconds":
                case "epoch-days":
                    break;
                default:
                    try {
                        DateTimeFormatter.ofPattern(pattern);
                    } catch (IllegalArgumentException e) {
                        String msg = String.format("The specified format was an invalid"
                                + " date time format: %s", formatText);
                        throw new JsonMappingException(p, msg, e);
                    }
            }
        } else {
            String msg = String.format("The type specified doesn't support format configuration: "
                    + "%s", type);
            throw new JsonMappingException(p, msg);
        }

        if (StringUtils.isNotBlank(afterTypeSignature)) {
            return formatText;
        }

        return String.format("[%s] %s", type.getTypeSignature().toString(),
                formatText);
    }
}
