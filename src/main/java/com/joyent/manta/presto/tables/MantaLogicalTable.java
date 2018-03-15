/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.column.MantaColumn;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Class representing the logical mapping of many file objects on Manta to a
 * single table with Presto.
 *
 * @since 1.0.0
 */
public class MantaLogicalTable implements Comparable<MantaLogicalTable> {
    /**
     * Unique name of table.
     */
    private final String tableName;

    /**
     * The root path in which all of the filters will be applied.
     */
    private final String rootPath;

    /**
     * The data type of which all files will conform.
     */
    private final MantaDataFileType dataFileType;

    /**
     * A regular expression for pre-filtering directories so that the contents
     * of the directories will not need to be listed.
     */
    private final Optional<MantaLogicalTablePartitionDefinition> partitionDefinition;

    /**
     * A JsonNode structure that contains the column definitions.
     */
    private final Optional<List<MantaColumn>> columnConfig;

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param tableName table name and schema that maps to the logical table
     * @param rootPath path in which all of the filters will be applied
     * @param dataFileType data type of which all files will conform
     */
    public MantaLogicalTable(final String tableName,
                             final String rootPath,
                             final MantaDataFileType dataFileType) {
        this.tableName = tableName;
        this.rootPath = rootPath;
        this.dataFileType = dataFileType;
        this.partitionDefinition = Optional.empty();
        this.columnConfig = Optional.empty();
    }
    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param tableName table name and schema that maps to the logical table
     * @param rootPath path in which all of the filters will be applied
     * @param dataFileType data type of which all files will conform
     * @param partitionDefinition  object representing partitioning scheme for table
     */
    public MantaLogicalTable(final String tableName,
                             final String rootPath,
                             final MantaDataFileType dataFileType,
                             final Optional<MantaLogicalTablePartitionDefinition> partitionDefinition) {
        this.tableName = tableName;
        this.rootPath = rootPath;
        this.dataFileType = dataFileType;
        this.partitionDefinition = partitionDefinition;
        this.columnConfig = Optional.empty();
    }
    /**
     * Creates a new instance based on the specified parameters. This
     * constructor is typically used by JSON deserialization.
     *
     * @param tableName table name and schema that maps to the logical table
     * @param rootPath path in which all of the filters will be applied
     * @param dataFileType data type of which all files will conform
     * @param partitionDefinition object representing partitioning scheme for table
     * @param columnConfig JsonNode object representing json that defines columns
     */
    @JsonCreator
    @SuppressWarnings("AvoidInlineConditionals")
    public MantaLogicalTable(@JsonProperty("name") final String tableName,
                             @JsonProperty("rootPath") final String rootPath,
                             @JsonProperty("dataFileType") final MantaDataFileType dataFileType,
                             @JsonProperty("partitionDefinition") final Optional<MantaLogicalTablePartitionDefinition> partitionDefinition,
                             @JsonProperty("columnConfig") final Optional<List<MantaColumn>> columnConfig) {
        this.tableName = Validate.notBlank(tableName, "table name must not be blank");
        this.rootPath = Validate.notBlank(rootPath, "root path must not be blank");
        this.dataFileType = Objects.requireNonNull(dataFileType, "data file type is null");
        this.partitionDefinition = partitionDefinition;
        this.columnConfig = columnConfig;
    }

    @JsonProperty("name")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getRootPath() {
        return rootPath;
    }

    @JsonProperty
    public MantaDataFileType getDataFileType() {
        return dataFileType;
    }

    @JsonProperty
    public Optional<List<MantaColumn>> getColumnConfig() {
        return columnConfig;
    }

    @JsonProperty
    public Optional<MantaLogicalTablePartitionDefinition> getPartitionDefinition() {
        return partitionDefinition;
    }

    /**
     * Predicate that applies the directory filter regex if it is not null.
     *
     * @return directory filter regex predicate or always true predicate
     */
    @JsonIgnore
    public Predicate<? super MantaObject> directoryFilter() {
        if (!partitionDefinition.isPresent()) {
            return mantaObject -> true;
        }

        Pattern directoryFilterRegex = partitionDefinition.get().getDirectoryFilterRegex();

        if (directoryFilterRegex == null) {
            return mantaObject -> true;
        }

        return (Predicate<MantaObject>) obj ->
                directoryFilterRegex.matcher(obj.getPath()).matches();
    }

    /**
     * Predicate that applies the root path filter regex if it is not null.
     *
     * @return filter regex predicate or always true predicate
     */
    @JsonIgnore
    public Predicate<? super MantaObject> filter() {
        if (!partitionDefinition.isPresent()) {
            return mantaObject -> true;
        }

        Pattern filterRegex = partitionDefinition.get().getFilterRegex();

        if (filterRegex == null) {
            return mantaObject -> true;
        }

        return (Predicate<MantaObject>) obj ->
                filterRegex.matcher(obj.getPath()).matches();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this)
                .append("tableName", tableName)
                .append("rootPath", rootPath)
                .append("dataFileType", dataFileType);

        if (partitionDefinition.isPresent()) {
            builder.append("partitionDefinition", partitionDefinition);
        } else {
            builder.append("partitionDefinition", "<empty>");
        }

        return builder.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MantaLogicalTable that = (MantaLogicalTable) o;
        return Objects.equals(tableName, that.tableName)
                && Objects.equals(rootPath, that.rootPath)
                && Objects.equals(dataFileType, that.dataFileType)
                && Objects.equals(partitionDefinition, that.partitionDefinition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, rootPath, dataFileType, partitionDefinition);
    }

    @Override
    public int compareTo(final MantaLogicalTable o) {
        return tableName.compareTo(o.tableName);
    }
}
