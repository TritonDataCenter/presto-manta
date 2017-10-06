/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.presto.column.RedirectingColumnLister;
import com.joyent.manta.presto.record.json.MantaJsonDataFileObjectMapperProvider;
import com.joyent.manta.presto.record.json.MantaJsonFileColumnLister;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaLogicalTableDeserializer;
import com.joyent.manta.presto.tables.MantaLogicalTableProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.joyent.manta.client.MantaClient.SEPARATOR;
import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;

/**
 * Guice module that loads in the configuration needed to inject the
 * dependencies for the Manta Presto Connector.
 */
public class MantaModule implements Module {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaModule.class);

    /**
     * Default maximum number of bytes per line is 10k.
     */
    private static final int DEFAULT_MAX_BYTES_PER_LINE = 10_240;

    private static final String MAX_BYTES_PER_LINE_KEY = "manta.max_bytes_per_line";

    private final String connectorId;
    private final TypeManager typeManager;
    private final ConfigContext config;
    private final Map<String, String> schemaMapping = new HashMap<>();
    private final Integer maxBytesPerLine;

    /**
     * Creates a new instance with the specified parameters.
     *
     * @param connectorId Presto connection id object for debugging
     * @param typeManager type manager associated with
     *                    Presto {@link .facebook.presto.spi.connector.ConnectorContext}
     * @param configParams Presto catalog configuration parameters
     */
    public MantaModule(final String connectorId,
                       final TypeManager typeManager,
                       final Map<String, String> configParams) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        requireNonNull(configParams, "Configuration is null");

        this.config = buildConfigContext(configParams);

        if (configParams.containsKey(MAX_BYTES_PER_LINE_KEY)) {
            maxBytesPerLine = Integer.parseInt(configParams.get(MAX_BYTES_PER_LINE_KEY));
        } else {
            maxBytesPerLine = DEFAULT_MAX_BYTES_PER_LINE;
        }

        LOG.debug("Manta Configuration: {}", this.config);
    }

    /**
     * Reads through the presto catalog configuration and maps the schema.
     *
     * @param config map to read configuration from
     * @param mapToUpdate map to add schema information to
     * @param homeDir directory to interpolate ~~ as
     */
    static void addToSchemaMapping(final Map<String, String> config,
        final Map<String, String> mapToUpdate, final String homeDir) {
        final int noOfDots = 3;

        for (final Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            String[] parts = StringUtils.split(key, ".", noOfDots);

            if (parts.length != noOfDots) {
                continue;
            }

            boolean isSchemaKey = parts[0].equals("manta") && parts[1].equals("schema");

            if (!isSchemaKey) {
                continue;
            }

            if (StringUtils.isNotBlank(parts[2]) && StringUtils.isNotBlank(val)) {
                final String path = MantaPrestoUtils.substitudeHomeDirectory(val, homeDir);
                mapToUpdate.put(parts[2], path);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("The following schema mappings were added:\n{}",
                    Joiner.on('\n').withKeyValueSeparator(": ").join(mapToUpdate));
        }
    }

    private ConfigContext buildConfigContext(final Map<String, String> configParams) {
        final ChainedConfigContext context;

        if (configParams != null && !configParams.isEmpty()) {
            context = new ChainedConfigContext(
                    new EnvVarConfigContext(),
                    new MapConfigContext(System.getProperties()),
                    new MapConfigContext(configParams),
                    new DefaultsConfigContext());

            ConfigContext.validate(context);

            String homeDir = context.getMantaHomeDirectory();
            addToSchemaMapping(configParams, schemaMapping, homeDir);
        } else {
            context = new ChainedConfigContext(
                    new EnvVarConfigContext(),
                    new MapConfigContext(System.getProperties()),
                    new DefaultsConfigContext());

            ConfigContext.validate(context);

            String homeDir = context.getMantaHomeDirectory();
            Map<String, String> defaultSchema = ImmutableMap.of(
                    "manta.schema.default", homeDir + SEPARATOR + "stor");
            addToSchemaMapping(defaultSchema, schemaMapping, homeDir);
        }

        return context;
    }

    @Override
    public void configure(final Binder binder) {
        binder.bind(new TypeLiteral<Map<String, String>>() { })
                .annotatedWith(Names.named("SchemaMapping"))
                .toInstance(ImmutableMap.copyOf(schemaMapping));

        binder.bind(Integer.class)
                .annotatedWith(Names.named("MaxBytesPerLine"))
                .toInstance(maxBytesPerLine);

        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(ObjectMapper.class)
                .annotatedWith(Names.named("JsonData"))
                .toProvider(MantaJsonDataFileObjectMapperProvider.class)
                .in(Scopes.SINGLETON);

        binder.bind(ConfigContext.class).toInstance(this.config);
        binder.bind(MantaClient.class).toProvider(MantaClientProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorAccessControl.class).to(MantaReadOnlyAccessControl.class).in(Scopes.SINGLETON);
        binder.bind(MantaConnector.class).in(Scopes.SINGLETON);
        binder.bind(MantaConnectorId.class).toInstance(new MantaConnectorId(connectorId));
        binder.bind(MantaLogicalTableProvider.class).in(Scopes.SINGLETON);
        binder.bind(RedirectingColumnLister.class).in(Scopes.SINGLETON);
        binder.bind(MantaJsonFileColumnLister.class).in(Scopes.SINGLETON);
        binder.bind(MantaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(MantaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MantaRecordSetProvider.class).in(Scopes.SINGLETON);

        /* We use a custom deserializer in order to provide more informative errors
         * and flexible parsing to users who are manually writing presto-tables.json
         * files. */
        jsonBinder(binder).addDeserializerBinding(MantaLogicalTable.class)
                .to(MantaLogicalTableDeserializer.class).in(Scopes.SINGLETON);
    }
}
