/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorTableHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaPrestoTableHandle implements ConnectorTableHandle {
    private final String objectPath;

    public MantaPrestoTableHandle(final MantaPrestoSchemaTableName tableName) {
        requireNonNull(tableName, "Table name is null");
        this.objectPath = tableName.getObjectPath();
    }

    public String getObjectPath() {
        return objectPath;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MantaPrestoTableHandle that = (MantaPrestoTableHandle) o;

        return Objects.equals(objectPath, that.objectPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectPath);
    }
}
