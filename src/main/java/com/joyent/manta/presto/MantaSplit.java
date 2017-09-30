/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * {@link ConnectorSplit} implementation that represent a single file object
 * within Manta.
 *
 * @since 1.0.0
 */
public class MantaSplit implements ConnectorSplit {
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String objectPath;
    private final MantaDataFileType dataFileType;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param connectorId presto connection id object for debugging
     * @param schemaName schema as defined in Presto catalog configuration
     * @param tableName table as defined in table definition file
     * @param objectPath path to object in Manta
     * @param dataFileType data type of all objects in table
     */
    @JsonCreator
    public MantaSplit(@JsonProperty("connectorId") final String connectorId,
                      @JsonProperty("schemaName") final String schemaName,
                      @JsonProperty("tableName") final String tableName,
                      @JsonProperty("objectPath") final String objectPath,
                      @JsonProperty("dataFileType") final MantaDataFileType dataFileType) {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.objectPath = requireNonNull(objectPath, "object path is null");
        this.dataFileType = requireNonNull(dataFileType, "data file type is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getObjectPath() {
        return objectPath;
    }

    @JsonProperty
    public MantaDataFileType getDataFileType() {
        return dataFileType;
    }

    @JsonProperty
    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        throw new UnsupportedOperationException("get Addresses is not supported");
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("connectorId", connectorId)
                .append("schemaName", schemaName)
                .append("tableName", tableName)
                .append("objectPath", objectPath)
                .append("dataFileType", dataFileType)
                .toString();
    }
}
