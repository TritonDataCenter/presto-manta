/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.tables;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.LinkedHashSet;
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
     * MantaLogicalTablePartitionDefinition
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
    @JsonDeserialize(as=LinkedHashSet.class)
    @JsonSerialize(as=LinkedHashSet.class)
    public LinkedHashSet<String> getDirectoryFilterPartitions() {
        return directoryFilterPartitions;
    }

    @JsonProperty
    @JsonDeserialize(as=LinkedHashSet.class)
    @JsonSerialize(as=LinkedHashSet.class)
    public LinkedHashSet<String> getFilterPartitions() {
        return filterPartitions;
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
