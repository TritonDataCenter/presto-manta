/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.google.common.io.Files;
import com.google.common.net.MediaType;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectResponse;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Predicate that filters directory listing to only the results that are
 * relevant to Presto.
 */
public class ObjectToTableDirectoryListingFilter implements Predicate<MantaObject> {
    /**
     * Manta client instance used for getting additional information about results.
     */
    private final MantaClient mantaClient;

    public ObjectToTableDirectoryListingFilter(final MantaClient mantaClient) {
        this.mantaClient = mantaClient;
    }

    @Override
    public boolean test(final MantaObject obj) {
        // We only want to list files, so we exclude directories
        if (obj.isDirectory()) {
            return false;
        }

        // If the file extension is supported, present the file as a table
        final String extension = Files.getFileExtension(obj.getPath());
        final boolean isSupportedExtension = StringUtils.isNotBlank(extension)
                && MantaPrestoFileType.isSupportedFileTypeByExtension(extension);

        if (isSupportedExtension) {
            return true;
        }

        /* Otherwise, as a last result, we try to parse the media type - but
         * we have to do a HEAD request to get at the content-type headers
         * because they aren't part of the directory listing results. */
        final String contentType;
        try {
            MantaObjectResponse info = mantaClient.head(obj.getPath());
            contentType = info.getContentType();
        } catch (IOException e) {
            return false;
        }

        if (StringUtils.isBlank(contentType)) {
            return false;
        }

        // Determine if the media type stored is of a supported value

        try {
            String mediaType = MantaPrestoUtils.extractMediaTypeFromContentType(
                    contentType);
            return MantaPrestoFileType.isSupportFileTypeByMediaType(mediaType);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
