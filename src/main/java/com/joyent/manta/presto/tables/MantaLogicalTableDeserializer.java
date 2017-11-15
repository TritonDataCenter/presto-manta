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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.MantaPrestoUtils;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import java.io.IOException;
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

        final Pattern directoryFilterRegex = readPattern(
                objectNode.get("directoryFilterRegex"), "directoryFilterRegex",
                p);

        final Pattern filterRegex = readPattern(objectNode.get("filterRegex"),
                "filterRegex", p);

        try {
            return new MantaLogicalTable(name, rootPath, dataFileType,
                    directoryFilterRegex, filterRegex);
        } catch (Exception e) {
            throw new JsonMappingException(p, "Unable to create new "
                    + "MantaLogicalTable instance", e);
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
}
