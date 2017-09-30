/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.exceptions;

import org.apache.commons.lang3.exception.ContextedException;
import org.apache.commons.lang3.exception.ExceptionContext;

/**
 * Base exception class for checked exceptions thrown within the Manta Presto
 * connector.
 *
 * @since 1.0.0
 */
public class MantaPrestoException extends ContextedException {
    private static final long serialVersionUID = -1779608808911106123L;

    public MantaPrestoException() {
    }

    public MantaPrestoException(final String message) {
        super(message);
    }

    public MantaPrestoException(final Throwable cause) {
        super(cause);
    }

    public MantaPrestoException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public MantaPrestoException(final String message, final Throwable cause,
                                final ExceptionContext context) {
        super(message, cause, context);
    }
}
