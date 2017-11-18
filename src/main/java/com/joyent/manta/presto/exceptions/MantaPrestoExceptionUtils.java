/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.exceptions;

import com.joyent.manta.client.MantaObject;
import org.apache.commons.lang3.exception.ExceptionContext;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Class providing utility functions for exception classes.
 *
 * @since 1.0.0
 */
public final class MantaPrestoExceptionUtils {
    /**
     * Private constructor because no non-static instances are needed.
     */
    private MantaPrestoExceptionUtils() {
    }

    /**
     * Adds details from a {@link MantaObject} to the error context.
     *
     * @param object object to pull details from
     * @param context exception to annotation with details
     */
    public static void annotateMantaObjectDetails(
            final MantaObject object, final ExceptionContext context) {
        if (object == null) {
            return;
        }

        context.setContextValue("path", object.getPath());
        context.setContextValue("contentLength", object.getContentLength());
        context.setContextValue("contentType", object.getContentType());
        context.setContextValue("etag", object.getEtag());
        context.setContextValue("lastModified", object.getLastModifiedTime());

        try {
            context.setContextValue("hostname", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException | NullPointerException e) {
            // Do nothing - just indicate that the hostname can't be known
            context.setContextValue("hostname", "unknown");
        }
    }
}
