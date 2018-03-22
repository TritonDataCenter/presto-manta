/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.google.common.net.MediaType;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

import static java.util.Objects.requireNonNull;

/**
 * Utility class that provides commonly used methods.
 *
 * @since 1.0.0
 */
public final class MantaPrestoUtils {
    /**
     * Private constructor because no non-static instances are needed.
     */
    private MantaPrestoUtils() {
    }

    /**
     * Extracts the media type without parameters from a HTTP content type.
     * @param contentType non-null content type as string
     * @return media type as string
     */
    public static String extractMediaTypeFromContentType(final String contentType) {
        requireNonNull(contentType, "Content type is null");

        return MediaType.parse(contentType).withoutParameters().toString();
    }

    /**
     * Extracts the character set from a content type.
     * @param contentType raw input content type to parse
     * @param defaultCharSet default character set to use when none can be parsed
     * @return content type's character set or default
     */
    public static Charset parseCharset(final String contentType, final Charset defaultCharSet) {
        if (StringUtils.isBlank(contentType)) {
            return defaultCharSet;
        }

        try {
            return MediaType.parse(contentType).charset().or(defaultCharSet);
        } catch (IllegalArgumentException e) {
            LoggerFactory.getLogger(MantaPrestoUtils.class)
                    .warn("Illegal character set on content-type: {}", contentType);
            return defaultCharSet;
        }
    }

    /**
     * Substitutes the actual home directory for ~~ in a given path.
     *
     * @param path path to substitute
     * @param homeDir home directory to replace ~~ with
     * @return interpolate path
     */
    public static String substituteHomeDirectory(final String path, final String homeDir) {
        requireNonNull(path, "Path is null");
        requireNonNull(homeDir, "Home directory is null");

        if (path.startsWith("~~")) {
            return MantaUtils.formatPath(homeDir + path.substring(2));
        } else {
            return MantaUtils.formatPath(path);
        }
    }
}
