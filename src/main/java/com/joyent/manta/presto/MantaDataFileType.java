/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.exceptions.MantaPrestoExceptionUtils;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Enum indicating the type of file that will be processed.
 *
 * @since 1.0.0
 */
public enum MantaDataFileType {
    /**
     * New line delimited JSON.
     */
    @JsonProperty("NDJSON")
    NDJSON(new String[] {"ndjson", "json", "ldjson"}, new String[] {
            "application/x-ndjson", "application/x-json-stream", "application/json"
    }),
    /**
     * Telegraf new line delimited JSON format data files.
     */
    TELEGRAF_NDJSON(new String[] {"telegraf.json", "telegraf.ndjson"},
            new String[] {"application/x-json-stream-telegraf"}),
    /**
     * Comma separated value.
     */
    @JsonProperty("CSV")
    CSV(new String[] {"csv"}, new String[] {"text/csv", "application/csv"});

    /**
     * File extensions related to enum value.
     */
    private final String[] extensions;

    /**
     * RFC4288 media types associated with file type.
     */
    private final String[] mediaTypes;

    /**
     * Lookup table to resolve enum value by extension.
     */
    private static final Map<String, MantaDataFileType> EXTENSION_LOOKUP;

    /**
     * Lookup table to resolve enum value by media type.
     */
    private static final Map<String, MantaDataFileType> MEDIA_TYPE_LOOKUP;

    static {
        ImmutableMap.Builder<String, MantaDataFileType> extensionMapBuilder =
                new ImmutableMap.Builder<>();
        ImmutableMap.Builder<String, MantaDataFileType> mediaTypeMapBuilder =
                new ImmutableMap.Builder<>();

        for (MantaDataFileType type : values()) {
            for (String extension : type.extensions) {
                extensionMapBuilder.put(extension, type);
            }
            for (String mediaType : type.mediaTypes) {
                mediaTypeMapBuilder.put(mediaType, type);
            }
        }
        EXTENSION_LOOKUP = extensionMapBuilder.build();
        MEDIA_TYPE_LOOKUP = mediaTypeMapBuilder.build();
    }

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param extensions file extensions to associate with data type
     * @param mediaTypes media types (from content-type) to associate with data-type
     */
    MantaDataFileType(final String[] extensions, final String[] mediaTypes) {
        this.extensions = extensions;
        this.mediaTypes = mediaTypes;
    }

    /**
     * @return the default file extension for the data file type
     */
    @JsonIgnore
    public String getDefaultExtension() {
        return extensions[0];
    }

    /**
     * @return the default media type for the data file type
     */
    @JsonIgnore
    public String getDefaultMediaType() {
        return mediaTypes[0];
    }

    /**
     * Resolves a enum value by file extension.
     *
     * @param extension file extension
     * @return related enum value or null if not found
     */
    public static MantaDataFileType valueByExtension(final String extension) {
        requireNonNull(extension, "Extension is null");
        return EXTENSION_LOOKUP.get(extension.toLowerCase());
    }

    /**
     * Resolves a enum value by media type.
     *
     * @param mediaType HTTP content type (media type)
     * @return related enum value or null if not found
     */
    public static MantaDataFileType valueByMediaType(final String mediaType) {
        requireNonNull(mediaType, "Media type is null");
        return MEDIA_TYPE_LOOKUP.get(mediaType.toLowerCase());
    }

    /**
     * Searches extentions, content-type and enum definition for matching
     * string and returns the data type associated with the search value.
     *
     * @param search string to match
     * @return matching enum or null if not found
     */
    public static MantaDataFileType searchAllValues(final String search) {
        MantaDataFileType fromExtension = valueByExtension(search);

        if (fromExtension != null) {
            return fromExtension;
        }

        MantaDataFileType fromMediaType = valueByMediaType(search);

        if (fromMediaType != null) {
            return fromMediaType;
        }

        try {
            return valueOf(search);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Checks the extension to see if it maps to a supported file type.
     *
     * @param extension extension to check
     * @return true if supported
     */
    public static boolean isSupportedFileTypeByExtension(final String extension) {
        return EXTENSION_LOOKUP.containsKey(extension.toLowerCase());
    }

    /**
     * Checks the media type to see if it maps to a supported file type.
     *
     * @param mediaType media type to check
     * @return true if supported
     */
    public static boolean isSupportedFileTypeByMediaType(final String mediaType) {
        return MEDIA_TYPE_LOOKUP.containsKey(mediaType.toLowerCase());
    }

    /**
     * Determines what {@link MantaDataFileType} is associated with
     * an object via looking up metadata on its file extension or
     * media type.
     *
     * @param object object reference for debugging information
     * @return instance of enum associated with object's file type
     */
    public static MantaDataFileType determineFileType(final MantaObject object) {
        requireNonNull(object, "Manta object is null");

        final String extension = Files.getFileExtension(object.getPath());
        final String mediaType = MantaPrestoUtils.extractMediaTypeFromContentType(
                object.getContentType());

        final MantaDataFileType type = ObjectUtils.firstNonNull(
                valueByExtension(extension), valueByMediaType(mediaType));

        if (type == null) {
            String msg = "Table name (Manta object) specified is not a "
                    + "supported file type";
            MantaPrestoIllegalArgumentException me = new MantaPrestoIllegalArgumentException(msg);
            MantaPrestoExceptionUtils.annotateMantaObjectDetails(object, me);
            throw me;
        }

        return type;
    }
}
