/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 *
 */
public class MantaPrestoClient {
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, MantaPrestoTable>>> schemas;

    @Inject
    public MantaPrestoClient(final MantaPrestoConfig config,
                             final JsonCodec<Map<String, List<MantaPrestoTable>>> catalogCodec)
            throws IOException {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        schemas = Suppliers.memoize(schemasSupplier(catalogCodec,
                URI.create(config.getMantaURL())));
    }

    public Set<String> getSchemaNames() {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(final String schema) {
        requireNonNull(schema, "schema is null");
        Map<String, MantaPrestoTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public MantaPrestoTable getTable(final String schema, final String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, MantaPrestoTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, MantaPrestoTable>>> schemasSupplier(
            final JsonCodec<Map<String, List<MantaPrestoTable>>> catalogCodec, final URI metadataUri) {
        return () -> {
            try {
                return lookupSchemas(metadataUri, catalogCodec);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static Map<String, Map<String, MantaPrestoTable>> lookupSchemas(
            final URI metadataUri, final JsonCodec<Map<String, List<MantaPrestoTable>>> catalogCodec)
            throws IOException {
        URL result = metadataUri.toURL();
        String json = Resources.toString(result, UTF_8);
        Map<String, List<MantaPrestoTable>> catalog = catalogCodec.fromJson(json);

        return ImmutableMap.copyOf(Maps.transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<MantaPrestoTable>, Map<String, MantaPrestoTable>> resolveAndIndexTables(final URI metadataUri) {
        return tables -> {
            Iterable<MantaPrestoTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(Maps.uniqueIndex(resolvedTables, MantaPrestoTable::getName));
        };
    }

    private static Function<MantaPrestoTable, MantaPrestoTable> tableUriResolver(final URI baseUri) {
        return table -> {
            List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            return new MantaPrestoTable(table.getName(), table.getColumns(), sources);
        };
    }
}
