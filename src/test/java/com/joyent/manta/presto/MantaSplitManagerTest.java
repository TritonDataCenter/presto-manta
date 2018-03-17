/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.presto.column.MantaPartitionColumn;
import com.joyent.manta.presto.tables.MantaLogicalTablePartitionDefinition;
import io.airlift.slice.Slices;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
public class MantaSplitManagerTest {
    private static final String FILE_REGEX = "^/.+/stor/dir/.+/analytics-(.+)-(.+)-(.+)\\.log\\..+$";
    private static final String DIR_REGEX = "^/.+/stor/dir/(.+)/.*$";
    private static final MantaLogicalTablePartitionDefinition PARTITION_DEFINITION = createPartitionDefinition();

    private static final Set<String> FAKE_OBJECTS = ImmutableSet.of(
            "/user",
            "/user/stor",
            "/user/stor/dir/server-1011",
            "/user/stor/dir/server-1011/analytics-1998-04-01.log.gz",
            "/user/stor/dir/server-1011/analytics-1998-04-02.log.gz",
            "/user/stor/dir/server-1012",
            "/user/stor/dir/server-1012/analytics-1998-04-01.log.gz",
            "/user/stor/dir/server-1012/analytics-1998-04-02.log.gz",
            "/user/stor/dir/server-1011/analytics-1998-05-10.log.gz",
            "/user/stor/dir/server-1011/analytics-1998-05-11.log.gz",
            "/user/stor/dir/server-1012/analytics-1998-05-11.log.gz",
            "/user/stor/dir/server-1011/analytics-1999-05-11.log.gz",
            "/user/stor/dir/server-1012/analytics-1999-05-11.log.gz"
    );

    public void filePartitionPredicateCorrectlyFilters() {
        final Pattern pattern = PARTITION_DEFINITION.getFilterRegex();
        final List<MantaPartitionColumn> partitionColumns =
                PARTITION_DEFINITION.filePartitionsAsColumns();

        final Map<ColumnHandle, Domain> domains = ImmutableMap.of(
                // year
                partitionColumns.get(0), Domain.singleValue(VarcharType.VARCHAR,
                        Slices.utf8Slice("1998")),
                // month
                partitionColumns.get(1), Domain.singleValue(VarcharType.VARCHAR,
                        Slices.utf8Slice("04"))
        );

        final Predicate<MantaObject> predicate = MantaSplitManager.createPartitionPredicate(
                pattern, domains, partitionColumns);

        List<String> filtered;

        try (Stream<MantaObject> objs = FAKE_OBJECTS.stream().map(fakeFileToObjectConverter)) {
            filtered = objs
                    .filter(predicate)
                    .filter(obj -> !obj.isDirectory())
                    .map(MantaObject::getPath).collect(Collectors.toList());
        }

        Assert.assertNotNull(filtered);
        Assert.assertFalse(filtered.isEmpty(), "Filtered list shouldn't be empty");

        List<String> expected = ImmutableList.of(
                "/user/stor/dir/server-1011/analytics-1998-04-01.log.gz",
                "/user/stor/dir/server-1011/analytics-1998-04-02.log.gz",
                "/user/stor/dir/server-1012/analytics-1998-04-01.log.gz",
                "/user/stor/dir/server-1012/analytics-1998-04-02.log.gz"
        );

        Assert.assertEquals(filtered, expected,
                "\nExpected: " + Joiner.on(", ").join(expected) + "\n"
                        + "Actual:   " + Joiner.on(", ").join(filtered) + "\n"
                        + "Error");
    }

    public void dirPartitionPredicateCorrectlyFilters() {
        final Pattern pattern = PARTITION_DEFINITION.getDirectoryFilterRegex();
        final List<MantaPartitionColumn> partitionColumns =
                PARTITION_DEFINITION.directoryPartitionsAsColumns();

        final Map<ColumnHandle, Domain> domains = ImmutableMap.of(
                // server
                partitionColumns.get(0), Domain.singleValue(VarcharType.VARCHAR,
                        Slices.utf8Slice("server-1012"))
        );

        final Predicate<MantaObject> predicate = MantaSplitManager.createPartitionPredicate(
                pattern, domains, partitionColumns);

        List<String> filtered;

        try (Stream<MantaObject> objs = FAKE_OBJECTS.stream().map(fakeFileToObjectConverter)) {
            filtered = objs
                    .filter(predicate)
                    .filter(MantaObject::isDirectory)
                    .map(MantaObject::getPath).collect(Collectors.toList());
        }

        Assert.assertNotNull(filtered);
        Assert.assertFalse(filtered.isEmpty(), "Filtered list shouldn't be empty");

        List<String> expected = ImmutableList.of(
                "/user",
                "/user/stor",
                "/user/stor/dir/server-1012"
        );

        Assert.assertEquals(filtered, expected,
                "\nExpected: " + Joiner.on(", ").join(expected) + "\n"
                       + "Actual:   " + Joiner.on(", ").join(filtered) + "\n"
                       + "Error");
    }

    private static Function<String, MantaObject> fakeFileToObjectConverter = s -> {
        final MantaObject obj = mock(MantaObject.class);

        when(obj.isDirectory()).thenReturn(!s.endsWith(".log.gz"));
        when(obj.getPath()).thenReturn(s);

        return obj;
    };

    static MantaLogicalTablePartitionDefinition createPartitionDefinition() {
        final LinkedHashSet<String> directoryFilterPartitions = new LinkedHashSet<>();
        directoryFilterPartitions.add("server");

        final LinkedHashSet<String> filterPartitions = new LinkedHashSet<>();
        filterPartitions.add("year");
        filterPartitions.add("month");
        filterPartitions.add("day");

        return new MantaLogicalTablePartitionDefinition(
                Pattern.compile(DIR_REGEX),
                Pattern.compile(FILE_REGEX),
                directoryFilterPartitions,
                filterPartitions);
    }
}
