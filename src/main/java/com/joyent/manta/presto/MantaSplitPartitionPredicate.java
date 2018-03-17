/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.column.MantaPartitionColumn;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link Predicate} implementation that allows for the filtering of
 * {@link MantaObject} instances based on the remote filename.
 *
 * @since 1.0.0
 */
public class MantaSplitPartitionPredicate implements Predicate<MantaObject> {
    /**
     * Utility singleton instance for a predicate that will conform to the type system
     * and will always return true.
     */
    public static final MantaSplitPartitionPredicate ALWAYS_TRUE =
            new MantaSplitPartitionPredicate(new LinkedHashMap<>(0), (Pattern)null);

    private final MantaPartitionColumn[] partitionColumns;
    private final String[] matchValues;
    private final Pattern partitionRegex;

    /**
     * Creates a new instance.
     *
     * @param partitionColumnToMatchValue Map containing the relation of the
     *                                    partition column to the user specified
     *                                    partition match value
     * @param partitionRegex Regular expression to use to extract groups to
     *                       partition on from filename
     */
    MantaSplitPartitionPredicate(final LinkedHashMap<MantaPartitionColumn, String> partitionColumnToMatchValue,
                                 final Pattern partitionRegex) {
        final int size = partitionColumnToMatchValue.size();
        this.partitionColumns = new MantaPartitionColumn[size];
        partitionColumnToMatchValue.keySet().toArray(this.partitionColumns);
        this.matchValues = new String[size];
        partitionColumnToMatchValue.values().toArray(this.matchValues);

        this.partitionRegex = partitionRegex;
    }

    /**
     * Creates a new instance.
     *
     * @param partitionColumns The partition columns the user specified
     * @param matchValues The actual values the user specified for matching the partitions
     * @param partitionRegexString Regular expression to use to extract groups to
     *                             partition on from filename
     */
    @JsonCreator
    public MantaSplitPartitionPredicate(
            @JsonProperty("partitionColumns") final MantaPartitionColumn[] partitionColumns,
            @JsonProperty("matchValues") final String[] matchValues,
            @JsonProperty("partitionRegex") final String partitionRegexString) {
        Validate.isTrue(partitionColumns.length == matchValues.length,
                "The size of the partitions column array isn't equal to match values");
        this.partitionColumns = partitionColumns;
        this.matchValues = matchValues;

        if (StringUtils.isNotBlank(partitionRegexString)) {
            this.partitionRegex = Pattern.compile(partitionRegexString);
        } else {
            this.partitionRegex = null;
        }
    }

    @Override
    public boolean test(final MantaObject obj) {
        if (partitionColumns.length == 0) {
            return true;
        }

        String filename = obj.getPath();

        if (obj.isDirectory()) {
            filename += MantaClient.SEPARATOR;
        }

        final Matcher matcher = partitionRegex.matcher(filename);

        /* If we don't match the regex for any groups, we let the file pass the
         * filter because it may be a directory or some other file that doesn't
         * match the criteria. If the user wants a general filter, that can
         * be defined as a "filter" in the logical table config.
         *
         * Typically, all directories, configuration files, etc, will be filtered
         * before this predicate is run.
         */
        if (!matcher.find()) {
            return true;
        }

        for (int i = 0; i < partitionColumns.length; i++) {
            /* We adjust the index by one because that's how groups in
             * regex patterns in Java count. */
            final int index = partitionColumns[i].getIndex() + 1;
            final String value = matchValues[i];

            // Error if the index values don't match
            if (index < 1 || index > matcher.groupCount()) {
                String msg = "Illegal index value based on parsed regular expression";
                MantaPrestoIllegalArgumentException e = new MantaPrestoIllegalArgumentException(msg);
                e.setContextValue("index", index);
                e.setContextValue("regex", partitionRegex.toString());
                e.setContextValue("input", filename);
                e.setContextValue("matcher.groupCount", matcher.groupCount());
                e.setContextValue("matcher", matcher.toString());
                throw e;
            }

            final String found = matcher.group(index);

            if (!found.equals(value)) {
                return false;
            }
        }

        return true;
    }

    @JsonProperty
    public MantaPartitionColumn[] getPartitionColumns() {
        return partitionColumns;
    }

    @JsonProperty
    public String[] getMatchValues() {
        return matchValues;
    }

    @JsonProperty
    public Pattern getPartitionRegex() {
        return partitionRegex;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MantaSplitPartitionPredicate that = (MantaSplitPartitionPredicate) o;

        return Arrays.equals(partitionColumns, that.partitionColumns)
               && Arrays.equals(matchValues, that.matchValues)
               && Objects.equals(partitionRegex.toString(), that.partitionRegex.toString());
    }

    @Override
    @SuppressWarnings("MagicNumber")
    public int hashCode() {
        int result = Objects.hash(partitionRegex);
        result = 31 * result + Arrays.hashCode(partitionColumns);
        result = 31 * result + Arrays.hashCode(matchValues);
        return result;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("partitionColumns", partitionColumns)
                .append("matchValues", matchValues)
                .append("partitionRegex", partitionRegex)
                .toString();
    }
}
