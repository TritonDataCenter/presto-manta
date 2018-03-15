/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.joyent.manta.presto.column.MantaPartitionColumn;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 *
 */
public class MantaLogicalTablePartitionDefinition {
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
     * Set of ordered partition names that correspond to the regex groups
     * provided in the diretoryFilterRegex.
     */
    private final LinkedHashSet<String> directoryFilterPartitions =
            new LinkedHashSet<>();

    /**
     * Set of ordered partition names that correspond to the regex groups
     * provided in the filterRegex.
     */
    private final LinkedHashSet<String> filterPartitions =
            new LinkedHashSet<>();
    /**
     * Creates a new instance with the specified parameters. This constructor
     * is used by Jackson to create instances.
     *
     * @param directoryFilterRegexString regular expression for pre-filtering directories
     * @param filterRegexString regular expression that will filter all results from the root path
     * @param directoryFilterPartitions set of ordered partition names that correspond to the regex groups
     *                                  provided in the diretoryFilterRegex
     * @param filterPartitions set of ordered partition names that correspond to the regex groups
     *                         provided in the filterRegex
     */
    @JsonCreator
    public MantaLogicalTablePartitionDefinition(
            @JsonProperty("directoryFilterRegex") final String directoryFilterRegexString,
            @JsonProperty("filterRegex") final String filterRegexString,
            @JsonProperty("directoryFilterPartitions") final LinkedHashSet<String> directoryFilterPartitions,
            @JsonProperty("filterPartitions") final LinkedHashSet<String> filterPartitions) {

        if (StringUtils.isNotBlank(directoryFilterRegexString)) {
            this.directoryFilterRegex = Pattern.compile(directoryFilterRegexString);
        } else {
            this.directoryFilterRegex = null;
        }

        if (StringUtils.isNotBlank(filterRegexString)) {
            this.filterRegex = Pattern.compile(filterRegexString);
        } else {
            this.filterRegex = null;
        }

        if (directoryFilterPartitions != null) {
            this.directoryFilterPartitions.addAll(directoryFilterPartitions);
        }
        if (filterPartitions != null) {
            this.filterPartitions.addAll(filterPartitions);
        }
    }

    /**
     * Creates a new instance with the specified parameters. This constructor
     * is used internally to create instances based on the actual class types
     * and not the string representation of the objects.
     *
     * @param directoryFilterRegex regular expression for pre-filtering directories
     * @param filterRegex regular expression that will filter all results from the root path
     * @param directoryFilterPartitions set of ordered partition names that correspond to the regex groups
     *                                  provided in the diretoryFilterRegex
     * @param filterPartitions set of ordered partition names that correspond to the regex groups
     *                         provided in the filterRegex
     */
    public MantaLogicalTablePartitionDefinition(final Pattern directoryFilterRegex,
                                                final Pattern filterRegex,
                                                final LinkedHashSet<String> directoryFilterPartitions,
                                                final LinkedHashSet<String> filterPartitions) {
        this.directoryFilterRegex = directoryFilterRegex;
        this.filterRegex = filterRegex;

        if (directoryFilterPartitions != null) {
            this.directoryFilterPartitions.addAll(directoryFilterPartitions);
        }
        if (filterPartitions != null) {
            this.filterPartitions.addAll(filterPartitions);
        }
    }

    @JsonProperty
    public Pattern getDirectoryFilterRegex() {
        return directoryFilterRegex;
    }

    @JsonProperty
    public Pattern getFilterRegex() {
        return filterRegex;
    }

    @JsonProperty
    @JsonDeserialize(as = LinkedHashSet.class)
    @JsonSerialize(as = LinkedHashSet.class)
    public LinkedHashSet<String> getDirectoryFilterPartitions() {
        return directoryFilterPartitions;
    }

    @JsonProperty
    @JsonDeserialize(as = LinkedHashSet.class)
    @JsonSerialize(as = LinkedHashSet.class)
    public LinkedHashSet<String> getFilterPartitions() {
        return filterPartitions;
    }

    /**
     * Generates a list of Manta columns based on the file partitions stored.
     *
     * @return list of Manta columns.
     */
    public List<MantaPartitionColumn> filePartitionsAsColumns() {
        return addPartitionColumns("file", getFilterRegex(),
                getFilterPartitions());
    }


    /**
     * Generates a list of Manta columns based on the directory partitions stored.
     *
     * @return list of Manta columns.
     */
    public List<MantaPartitionColumn> directoryPartitionsAsColumns() {
        return addPartitionColumns("directory",
                getDirectoryFilterRegex(), getDirectoryFilterPartitions());
    }

    private List<MantaPartitionColumn> addPartitionColumns(final String partitionType,
                                                           final Pattern regex,
                                                           final LinkedHashSet<String> partitions) {
        final ImmutableList.Builder<MantaPartitionColumn> columns =
                new ImmutableList.Builder<>();

        if (regex == null) {
            return Collections.emptyList();
        }

        int index = 0;

        for (String columnName : partitions) {
            final MantaPartitionColumn column = createColumnBasedOnPartition(
                    columnName, partitionType, index++, regex.toString());
            columns.add(column);
        }

        return columns.build();
    }

    private static MantaPartitionColumn createColumnBasedOnPartition(final String partitionName,
                                                                     final String partitionType,
                                                                     final int index,
                                                                     final String regex) {
        final String comment = String.format("%s partition match [%s] index [%d]",
                partitionType, regex, index);
        return new MantaPartitionColumn(index, partitionName, VarcharType.VARCHAR, comment,
                null, true, null);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MantaLogicalTablePartitionDefinition that = (MantaLogicalTablePartitionDefinition) o;

        boolean directoryFilterRegexEqual =
                Objects.equals(Objects.toString(directoryFilterRegex, "<null>"),
                        Objects.toString(that.directoryFilterRegex, "<null>"));

        boolean filterRegexEqual =
                Objects.equals(Objects.toString(filterRegex, "<null>"),
                        Objects.toString(that.filterRegex, "<null>"));

        return directoryFilterRegexEqual
                && filterRegexEqual
                && Objects.equals(directoryFilterPartitions, that.directoryFilterPartitions)
                && Objects.equals(filterPartitions, that.filterPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directoryFilterRegex, filterRegex,
                directoryFilterPartitions, filterPartitions);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("directoryFilterRegex", directoryFilterRegex)
                .append("filterRegex", filterRegex)
                .append("directoryFilterPartitions", directoryFilterPartitions)
                .append("filterPartitions", filterPartitions)
                .toString();
    }
}
