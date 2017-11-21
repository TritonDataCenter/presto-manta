/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.exceptions;

import com.joyent.manta.client.MantaObject;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.MantaVersion;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.slf4j.MDC;

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
        context.setContextValue("mantaSdkVersion", MantaVersion.VERSION);
        context.setContextValue("hostname", findHostname());

        if (object == null) {
            return;
        }

        context.setContextValue("path", object.getPath());
        context.setContextValue("contentLength", object.getContentLength());
        context.setContextValue("contentType", object.getContentType());
        context.setContextValue("etag", object.getEtag());
        context.setContextValue("lastModified", object.getLastModifiedTime());
        context.setContextValue("responseTime", object.getHeaderAsString("x-response-time"));
        context.setContextValue("server", object.getHeaderAsString("x-server-name"));
        context.setContextValue("requestId", object.getHeaderAsString(MantaHttpHeaders.REQUEST_ID));
        /* This is a crude hack that gets the load balancer address from the
         * SLF4J logger as it was set within the Manta SDK. */
        context.setContextValue("loadBalancer", MDC.get("loadBalancerAddress"));
    }

    private static String findHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException | NullPointerException e) {
            // Do nothing - just indicate that the hostname can't be known
            return "unknown";
        }
    }
}
