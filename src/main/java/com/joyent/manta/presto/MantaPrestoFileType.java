/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Enum indicating the type of file that will be processed.
 */
public enum MantaPrestoFileType {
    /**
     * New line delimited JSON.
     */
    LDJSON(new String[] {"json", "ndjson"}, new String[] {"application/x-ndjson", "x-json-stream", "application/json"}),
    /**
     * Comma separated value.
     */
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
    private static final Map<String, MantaPrestoFileType> EXTENSION_LOOKUP;

    /**
     * Lookup table to resolve enum value by media type.
     */
    private static final Map<String, MantaPrestoFileType> MEDIA_TYPE_LOOKUP;

    static {
        ImmutableMap.Builder<String, MantaPrestoFileType> extensionMapBuilder =
                new ImmutableMap.Builder<>();
        ImmutableMap.Builder<String, MantaPrestoFileType> mediaTypeMapBuilder =
                new ImmutableMap.Builder<>();

        for (MantaPrestoFileType type : values()) {
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

    MantaPrestoFileType(final String[] extensions, final String[] mediaTypes) {
        this.extensions = extensions;
        this.mediaTypes = mediaTypes;
    }

    /**
     * Resolves a enum value by file extension.
     *
     * @param extension file extension
     * @return related enum value or null if not found
     */
    public static MantaPrestoFileType valueByExtension(final String extension) {
        requireNonNull(extension, "Extension is null");
        return EXTENSION_LOOKUP.get(extension);
    }

    /**
     * Resolves a enum value by media type.
     *
     * @param mediaType HTTP content type (media type)
     * @return related enum value or null if not found
     */
    public static MantaPrestoFileType valueByMediaType(final String mediaType) {
        requireNonNull(mediaType, "Media type is null");
        return MEDIA_TYPE_LOOKUP.get(mediaType);
    }

    /**
     * Checks the extension to see if it maps to a supported file type.
     *
     * @param extension extension to check
     * @return true if supported
     */
    public static boolean isSupportedFileTypeByExtension(final String extension) {
        return EXTENSION_LOOKUP.containsKey(extension);
    }

    /**
     * Checks the media type to see if it maps to a supported file type.
     *
     * @param mediaType media type to check
     * @return true if supported
     */
    public static boolean isSupportFileTypeByMediaType(final String mediaType) {
        return MEDIA_TYPE_LOOKUP.containsKey(mediaType);
    }
}
