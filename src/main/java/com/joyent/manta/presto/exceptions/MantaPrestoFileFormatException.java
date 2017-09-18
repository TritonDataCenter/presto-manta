/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.exceptions;

import org.apache.commons.lang3.exception.ExceptionContext;

/**
 * Exception representing a problem with the format of a data file being read
 * as a table
 */
public class MantaPrestoFileFormatException extends MantaPrestoRuntimeException {
    private static final long serialVersionUID = -7134125396742967590L;

    public MantaPrestoFileFormatException() {
    }

    public MantaPrestoFileFormatException(final String message) {
        super(message);
    }

    public MantaPrestoFileFormatException(final Throwable cause) {
        super(cause);
    }

    public MantaPrestoFileFormatException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public MantaPrestoFileFormatException(final String message, final Throwable cause,
                                          final ExceptionContext context) {
        super(message, cause, context);
    }
}
