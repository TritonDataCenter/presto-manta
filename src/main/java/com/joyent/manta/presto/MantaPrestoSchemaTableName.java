/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

import java.util.Objects;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * A Manta Presto specific implementation of {@link SchemaTableName} that
 * preserves the directory path structure with case sensitivity.
 */
public class MantaPrestoSchemaTableName extends SchemaTableName
        implements ConnectorTableHandle {
    private final String directory;
    private final String relativeFilePath;

    public MantaPrestoSchemaTableName(final String schemaName,
                                      final String tableName,
                                      final String directory,
                                      final String relativeFilePath) {
        super(schemaName, tableName);
        this.directory = directory;
        this.relativeFilePath = relativeFilePath;
    }

    @Override
    public SchemaTablePrefix toSchemaTablePrefix() {
        return new SchemaTablePrefix(directory, relativeFilePath);
    }

    public String getDirectory() {
        return directory;
    }

    public String getRelativeFilePath() {
        return relativeFilePath;
    }

    /**
     * @return the full path to the Manta relativeFilePath include the directory
     */
    public String getObjectPath() {
        return getDirectory() + SEPARATOR + getRelativeFilePath();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) return false;

        final MantaPrestoSchemaTableName that = (MantaPrestoSchemaTableName) o;
        return Objects.equals(directory, that.directory)
               && Objects.equals(relativeFilePath, that.relativeFilePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), directory, relativeFilePath);
    }
}
