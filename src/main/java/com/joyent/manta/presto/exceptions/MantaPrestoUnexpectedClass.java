/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.exceptions;

/**
 * Runtime exception thrown when the wrong instance of a class is passed as an
 * argument to a method.
 */
public class MantaPrestoUnexpectedClass extends MantaPrestoIllegalArgumentException {
    private static final long serialVersionUID = 3689129445019722088L;

    /**
     * Creates a new instance of the exception passed on the passed class
     * expectations.
     *
     * @param expectedClass the class expected
     * @param actualClass the actual class
     */
    public MantaPrestoUnexpectedClass(final Class<?> expectedClass,
                                      final Class<?> actualClass) {
        super("Unexpected class encountered");

        if (expectedClass != null) {
            setContextValue("expectedClass", expectedClass.getName());
        }

        if (actualClass != null) {
            setContextValue("actualClass", actualClass.getName());
        }
    }
}
