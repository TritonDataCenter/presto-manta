/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 *
 */
public class MantaPrestoTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final MantaPrestoTableHandle table;

    @JsonCreator
    public MantaPrestoTableLayoutHandle(@JsonProperty("table") MantaPrestoTableHandle table) {
        this.table = table;
    }

    @JsonProperty
    public MantaPrestoTableHandle getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MantaPrestoTableLayoutHandle that = (MantaPrestoTableLayoutHandle) o;

        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table);
    }

    @Override
    public String toString() {
        return table.toString();
    }
}
