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
import com.google.common.base.Predicates;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.MantaDataFileType;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;
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
    private final Pattern directoryFilterRegex;

    /**
     * A regular expression that will filter all results from the root path.
     */
    private final Pattern filterRegex;

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
        this(tableName, rootPath, dataFileType, (Pattern)null, null);
    }

    /**
     * Creates a new instance based on the specified parameters. This
     * constructor is typically used by JSON deserialization.
     *
     * @param tableName table name and schema that maps to the logical table
     * @param rootPath path in which all of the filters will be applied
     * @param dataFileType data type of which all files will conform
     * @param directoryFilterRegex regex for pre-filtering directories
     * @param filterRegex regex for filtering all dirs and files from root path
     */
    @JsonCreator
    @SuppressWarnings("AvoidInlineConditionals")
    public MantaLogicalTable(@JsonProperty("name") final String tableName,
                             @JsonProperty("rootPath") final String rootPath,
                             @JsonProperty("dataFileType") final MantaDataFileType dataFileType,
                             @JsonProperty("directoryFilterRegex") final String directoryFilterRegex,
                             @JsonProperty("filterRegex") final String filterRegex) {
        this(tableName, rootPath, dataFileType,
                directoryFilterRegex == null ? null : Pattern.compile(directoryFilterRegex),
                filterRegex == null ? null : Pattern.compile(filterRegex));
    }

    /**
     * Creates a new instance based on the specified parameters.
     *
     * @param tableName table name and schema that maps to the logical table
     * @param rootPath path in which all of the filters will be applied
     * @param dataFileType data type of which all files will conform
     * @param directoryFilterRegex regex for pre-filtering directories
     * @param filterRegex regex for filtering all dirs and files from root path
     */
    public MantaLogicalTable(final String tableName,
                             final String rootPath,
                             final MantaDataFileType dataFileType,
                             final Pattern directoryFilterRegex,
                             final Pattern filterRegex) {
        this.tableName = Validate.notBlank(tableName, "table name must not be blank");
        this.rootPath = Validate.notBlank(rootPath, "root path must not be blank");
        this.dataFileType = Objects.requireNonNull(dataFileType, "data file type is null");
        this.directoryFilterRegex = directoryFilterRegex;
        this.filterRegex = filterRegex;
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
    public Pattern getDirectoryFilterRegex() {
        return directoryFilterRegex;
    }

    /**
     * Predicate that applies the directory filter regex if it is not null.
     *
     * @return directory filter regex predicate or always true predicate
     */
    @JsonIgnore
    public Predicate<? super MantaObject> directoryFilter() {
        if (directoryFilterRegex != null) {
            return (Predicate<MantaObject>) obj ->
                    directoryFilterRegex.matcher(obj.getPath()).matches();
        }

        return Predicates.alwaysTrue();
    }

    @JsonProperty
    public Pattern getFilterRegex() {
        return filterRegex;
    }

    /**
     * Predicate that applies the root path filter regex if it is not null.
     *
     * @return filter regex predicate or always true predicate
     */
    @JsonIgnore
    public Predicate<? super MantaObject> filter() {
        if (filterRegex != null) {
            return (Predicate<MantaObject>) obj ->
                    filterRegex.matcher(obj.getPath()).matches();
        }

        return Predicates.alwaysTrue();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tableName", tableName)
                .append("rootPath", rootPath)
                .append("dataFileType", dataFileType)
                .append("directoryFilterRegex", directoryFilterRegex)
                .append("filterRegex", filterRegex)
                .toString();
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
                && Objects.equals(Objects.toString(directoryFilterRegex, "<null>"),
                                  Objects.toString(that.directoryFilterRegex, "<null>"))
                && Objects.equals(Objects.toString(filterRegex, "<null>"),
                                  Objects.toString(that.filterRegex, "<null>"));
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName,
                rootPath, dataFileType,
                directoryFilterRegex, filterRegex);
    }

    @Override
    public int compareTo(final MantaLogicalTable o) {
        return tableName.compareTo(o.tableName);
    }
}
