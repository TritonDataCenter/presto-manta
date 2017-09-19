/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.google.common.net.MediaType;

import static java.util.Objects.requireNonNull;

/**
 * Utility class that provides commonly used methods.
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
     */
    public static String extractMediaTypeFromContentType(final String contentType) {
        requireNonNull(contentType, "Content type is null");

        return MediaType.parse(contentType).withoutParameters().toString();
    }
}
