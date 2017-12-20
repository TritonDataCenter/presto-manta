/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.record.telegraf;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.joyent.manta.presto.column.MantaColumn;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

@Test
public class MantaTelegrafColumnListerTest {
    public void hasExpectedColumns() {
        final MantaTelegrafColumnLister columnLister = new MantaTelegrafColumnLister();
        final ConnectorSession session = new TestingConnectorSession(Collections.emptyList());
        List<MantaColumn> columns = columnLister.listColumns(null, null, session);

        Assert.assertEquals(columns.get(0).getName(), "timestamp");
        Assert.assertEquals(columns.get(1).getName(), "tags");
        Assert.assertEquals(columns.get(2).getName(), "name");
        Assert.assertEquals(columns.get(3).getName(), "fields");
    }
}
