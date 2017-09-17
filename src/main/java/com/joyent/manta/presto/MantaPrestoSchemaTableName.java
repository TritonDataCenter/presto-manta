/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joyent.manta.util.MantaUtils;

import java.nio.file.Paths;
import java.util.Objects;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * A Manta Presto specific implementation of {@link SchemaTableName} that
 * preserves the directory path structure with case sensitivity.
 */
public class MantaPrestoSchemaTableName extends SchemaTableName {
    private final String directory;
    private final String file;

    public MantaPrestoSchemaTableName(final SchemaTableName tableName) {
        this(tableName.getSchemaName(), tableName.getTableName());
    }

    public MantaPrestoSchemaTableName(final String objectPath) {
        this(Paths.get(objectPath).getParent().toString(),
                MantaUtils.lastItemInPath(objectPath));
    }

    @JsonCreator
    public MantaPrestoSchemaTableName(@JsonProperty("schema") final String schemaName,
                                      @JsonProperty("table") final String tableName) {
        super(MantaUtils.formatPath(schemaName), tableName);
        this.directory = MantaUtils.formatPath(schemaName);
        this.file = tableName;
    }

    public String getDirectory() {
        return directory;
    }

    public String getFile() {
        return file;
    }

    /**
     * @return the full path to the Manta file include the directory
     */
    public String getObjectPath() {
        return getDirectory() + SEPARATOR + getFile();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MantaPrestoSchemaTableName that = (MantaPrestoSchemaTableName) o;

        return Objects.equals(directory, that.directory)
               && Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directory, file);
    }
}
