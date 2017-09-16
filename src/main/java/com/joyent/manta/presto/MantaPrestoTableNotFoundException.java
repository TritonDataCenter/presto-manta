/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import org.apache.commons.lang3.exception.ExceptionContext;

/**
 * Runtime exception indicating that a table couldn't be found at the specified
 * path.
 */
public class MantaPrestoTableNotFoundException extends MantaPrestoRuntimeException {
    private static final long serialVersionUID = -4293252008339636338L;

    public MantaPrestoTableNotFoundException() {
    }

    public MantaPrestoTableNotFoundException(final String message) {
        super(message);
    }

    public MantaPrestoTableNotFoundException(final Throwable cause) {
        super(cause);
    }

    public MantaPrestoTableNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public MantaPrestoTableNotFoundException(final String message, final Throwable cause,
                                             final ExceptionContext context) {
        super(message, cause, context);
    }
}
