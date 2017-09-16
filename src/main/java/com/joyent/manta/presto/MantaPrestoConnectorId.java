/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Class representing the id of a {@link MantaPrestoConnector}.
 */
public class MantaPrestoConnectorId {
    /**
     * Connector identifier.
     */
    private final String id;

    /**
     * Creates a new instance with the specified identifier.
     *
     * @param id Connector identifier
     */
    public MantaPrestoConnectorId(final String id) {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public String toString() {
        return this.id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MantaPrestoConnectorId that = (MantaPrestoConnectorId) o;

        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
