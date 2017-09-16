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
 * Runtime exception indicating that a schema couldn't be found at the specified
 * path.
 */
public class MantaPrestoSchemaNotFoundException extends MantaPrestoRuntimeException {
    private static final long serialVersionUID = 1967487001003925699L;

    public MantaPrestoSchemaNotFoundException() {
    }

    public MantaPrestoSchemaNotFoundException(final String message) {
        super(message);
    }

    public MantaPrestoSchemaNotFoundException(final Throwable cause) {
        super(cause);
    }

    public MantaPrestoSchemaNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public MantaPrestoSchemaNotFoundException(final String message, final Throwable cause,
                                              final ExceptionContext context) {
        super(message, cause, context);
    }
}
