/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.type.TypeManager;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
public class MantaConnectorFactoryTest {

    public void canCreateNewConnector() {
        String connectorId = String.format("inject-test-id-%s", UUID.randomUUID());
        ConnectorContext context = mock(ConnectorContext.class);
        TypeManager typeManager = mock(TypeManager.class);
        when(context.getTypeManager()).thenReturn(typeManager);

        MantaConnectorFactory connectorFactory = new MantaConnectorFactory();
        Connector connector = connectorFactory.create(
                connectorId, MantaPrestoTestUtils.UNIT_TEST_CONFIG, context);
        Assert.assertNotNull(connector);

        connector.shutdown();
    }
}
