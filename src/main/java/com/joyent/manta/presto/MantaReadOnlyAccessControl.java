/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;

import java.util.Set;

import static com.facebook.presto.spi.security.AccessDeniedException.*;

/**
 * Implementation of {@link ConnectorAccessControl} specific for enforcing
 * read-only access to Manta.
 */
class MantaReadOnlyAccessControl implements ConnectorAccessControl {
    /**
     * Creates a new access control instance.
     */
    MantaReadOnlyAccessControl() {
    }

    @Override
    public void checkCanShowSchemas(final ConnectorTransactionHandle transactionHandle,
                                    final Identity identity) {
    }

    @Override
    public Set<String> filterSchemas(final ConnectorTransactionHandle transactionHandle,
                                     final Identity identity,
                                     final Set<String> schemaNames) {
        return schemaNames;
    }

    @Override
    public void checkCanAddColumn(final ConnectorTransactionHandle transaction,
                                  final Identity identity,
                                  final SchemaTableName tableName) {
        denyAddColumn(tableName.toString());
    }

    @Override
    public void checkCanDropColumn(final ConnectorTransactionHandle transactionHandle,
                                   final Identity identity,
                                   final SchemaTableName tableName) {
        denyDropColumn(tableName.toString());
    }

    @Override
    public void checkCanCreateTable(final ConnectorTransactionHandle transaction,
                                    final Identity identity,
                                    final SchemaTableName tableName) {
        denyCreateTable(tableName.toString());
    }

    @Override
    public void checkCanDropTable(final ConnectorTransactionHandle transaction,
                                  final Identity identity,
                                  final SchemaTableName tableName) {
        denyDropTable(tableName.toString());
    }

    @Override
    public void checkCanRenameTable(final ConnectorTransactionHandle transaction,
                                    final Identity identity,
                                    final SchemaTableName tableName,
                                    final SchemaTableName newTableName) {
        denyRenameTable(tableName.toString(), newTableName.toString());
    }

    @Override
    public void checkCanShowTablesMetadata(final ConnectorTransactionHandle transactionHandle,
                                           final Identity identity,
                                           final String schemaName) {
    }

    @Override
    public Set<SchemaTableName> filterTables(final ConnectorTransactionHandle transactionHandle,
                                             final Identity identity,
                                             final Set<SchemaTableName> tableNames) {
        return tableNames;
    }

    @Override
    public void checkCanRenameColumn(final ConnectorTransactionHandle transaction,
                                     final Identity identity,
                                     final SchemaTableName tableName) {
        denyRenameColumn(tableName.toString());
    }

    @Override
    public void checkCanSelectFromTable(final ConnectorTransactionHandle transaction,
                                        final Identity identity,
                                        final SchemaTableName tableName) {
        // ALLOW ACCESS TO ALL TABLES
    }

    @Override
    public void checkCanInsertIntoTable(final ConnectorTransactionHandle transaction,
                                        final Identity identity,
                                        final SchemaTableName tableName) {
        denyInsertTable(tableName.toString());
    }

    @Override
    public void checkCanDeleteFromTable(final ConnectorTransactionHandle transaction,
                                        final Identity identity,
                                        final SchemaTableName tableName) {
        denyDeleteTable(tableName.toString());
    }

    @Override
    public void checkCanCreateView(final ConnectorTransactionHandle transaction,
                                   final Identity identity,
                                   final SchemaTableName viewName) {
        // ALLOW - BUT WILL FAIL WITH NOT SUPPORTED MESSAGE
    }

    @Override
    public void checkCanDropView(final ConnectorTransactionHandle transaction,
                                 final Identity identity,
                                 final SchemaTableName viewName) {
        denyDropView(viewName.toString());
    }

    @Override
    public void checkCanSelectFromView(final ConnectorTransactionHandle transaction,
                                       final Identity identity,
                                       final SchemaTableName viewName) {
        // ALLOW ACCESS TO SELECT FROM VIEW - EVEN IF VIEWS AREN'T SUPPORTED
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(final ConnectorTransactionHandle transaction,
                                                      final Identity identity,
                                                      final SchemaTableName tableName) {
        // ALLOW
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(final ConnectorTransactionHandle transaction,
                                                     final Identity identity,
                                                     final SchemaTableName viewName) {
        // ALLOW
    }

    @Override
    public void checkCanSetCatalogSessionProperty(final Identity identity,
                                                  final String propertyName) {
        // ALLOW SETTING SESSION PROPERTIES
    }

    @Override
    public void checkCanGrantTablePrivilege(final ConnectorTransactionHandle transaction,
                                            final Identity identity,
                                            final Privilege privilege,
                                            final SchemaTableName tableName,
                                            final String grantee,
                                            final boolean withGrantOption) {
        denyGrantTablePrivilege(privilege.name(), tableName.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(final ConnectorTransactionHandle transaction,
                                             final Identity identity,
                                             final Privilege privilege,
                                             final SchemaTableName tableName,
                                             final String revokee,
                                             final boolean grantOptionFor) {
        denyRevokeTablePrivilege(privilege.name(), tableName.toString());
    }
}
