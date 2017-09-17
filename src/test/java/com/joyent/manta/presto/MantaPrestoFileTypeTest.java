/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class MantaPrestoFileTypeTest {
    public void canSelectNDJsonFileTypeByJsonExtension() {
        MantaPrestoFileType type = MantaPrestoFileType.valueByExtension("json");
        Assert.assertEquals(type, MantaPrestoFileType.LDJSON);
    }

    public void canSelectNDJsonFileTypeByNDJsonExtension() {
        MantaPrestoFileType type = MantaPrestoFileType.valueByExtension("ndjson");
        Assert.assertEquals(type, MantaPrestoFileType.LDJSON);
    }

    public void canSelectCSVFileTypeByExtension() {
        MantaPrestoFileType type = MantaPrestoFileType.valueByExtension("csv");
        Assert.assertEquals(type, MantaPrestoFileType.CSV);
    }

    public void willReturnNullOnUnknownExtension() {
        Assert.assertNull(MantaPrestoFileType.valueByExtension("log"));
    }

    public void canSelectNDJsonByJsonMediaType() {
        MantaPrestoFileType type = MantaPrestoFileType.valueByMediaType("application/json");
        Assert.assertEquals(type, MantaPrestoFileType.LDJSON);
    }

    public void canSelectNDJsonByNDJsonMediaType() {
        MantaPrestoFileType type = MantaPrestoFileType.valueByMediaType("application/x-ndjson");
        Assert.assertEquals(type, MantaPrestoFileType.LDJSON);
    }

    public void canSelectCSVByApplicationCSVMediaType() {
        MantaPrestoFileType type = MantaPrestoFileType.valueByMediaType("application/csv");
        Assert.assertEquals(type, MantaPrestoFileType.CSV);
    }

    public void canSelectCSVByTextCSVMediaType() {
        MantaPrestoFileType type = MantaPrestoFileType.valueByMediaType("text/csv");
        Assert.assertEquals(type, MantaPrestoFileType.CSV);
    }

    public void willReturnNullOnUnknownMediaType() {
        Assert.assertNull(MantaPrestoFileType.valueByMediaType("text/plain"));
    }
}
