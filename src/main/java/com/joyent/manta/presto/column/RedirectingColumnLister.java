/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.joyent.manta.presto.MantaConnectorId;
import com.joyent.manta.presto.MantaDataFileType;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import com.joyent.manta.presto.record.json.MantaJsonFileColumnLister;
import com.joyent.manta.presto.tables.MantaLogicalTable;
import com.joyent.manta.presto.tables.MantaSchemaTableName;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Class that given a {@link MantaSchemaTableName} and a {@link MantaLogicalTable}
 * will provide a list of all columns associated with a table by doing operations
 * specific to the data type of the files associated with the table.
 *
 * @since 1.0.0
 */
public class RedirectingColumnLister implements ColumnLister {
    private final MantaConnectorId connectorId;
    private final MantaJsonFileColumnLister jsonLister;

    /**
     * Creates a new instance with the required properties.
     *
     * @param connectorId presto connection id object for debugging
     * @param jsonLister lister instance for processing JSON columns
     */
    @Inject
    public RedirectingColumnLister(final MantaConnectorId connectorId,
                                   final MantaJsonFileColumnLister jsonLister) {
        this.connectorId = requireNonNull(connectorId, "Connector id is null");
        this.jsonLister = requireNonNull(jsonLister, "Json lister is null");
    }

    @Override
    public List<MantaColumn> listColumns(final MantaSchemaTableName tableName,
                                         final MantaLogicalTable table) {
        final MantaDataFileType type = table.getDataFileType();

        final ColumnLister lister;

        switch (type) {
            case NDJSON:
                lister = jsonLister;
                break;
            case CSV:
            default:
                String msg = "Unknown file type enum resolved";
                MantaPrestoIllegalArgumentException me = new MantaPrestoIllegalArgumentException(msg);
                me.addContextValue("type", type);
                me.setContextValue("connectorId", connectorId);
                throw me;
        }

        return lister.listColumns(tableName, table);
    }
}
