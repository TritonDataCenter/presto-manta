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
public class MantaDataFileTypeTest {
    public void canSelectNDJsonFileTypeByJsonExtension() {
        MantaDataFileType type = MantaDataFileType.valueByExtension("json");
        Assert.assertEquals(type, MantaDataFileType.LDJSON);
    }

    public void canSelectNDJsonFileTypeByNDJsonExtension() {
        MantaDataFileType type = MantaDataFileType.valueByExtension("ndjson");
        Assert.assertEquals(type, MantaDataFileType.LDJSON);
    }

    public void canSelectCSVFileTypeByExtension() {
        MantaDataFileType type = MantaDataFileType.valueByExtension("csv");
        Assert.assertEquals(type, MantaDataFileType.CSV);
    }

    public void willReturnNullOnUnknownExtension() {
        Assert.assertNull(MantaDataFileType.valueByExtension("log"));
    }

    public void canSelectNDJsonByJsonMediaType() {
        MantaDataFileType type = MantaDataFileType.valueByMediaType("application/json");
        Assert.assertEquals(type, MantaDataFileType.LDJSON);
    }

    public void canSelectNDJsonByNDJsonMediaType() {
        MantaDataFileType type = MantaDataFileType.valueByMediaType("application/x-ndjson");
        Assert.assertEquals(type, MantaDataFileType.LDJSON);
    }

    public void canSelectCSVByApplicationCSVMediaType() {
        MantaDataFileType type = MantaDataFileType.valueByMediaType("application/csv");
        Assert.assertEquals(type, MantaDataFileType.CSV);
    }

    public void canSelectCSVByTextCSVMediaType() {
        MantaDataFileType type = MantaDataFileType.valueByMediaType("text/csv");
        Assert.assertEquals(type, MantaDataFileType.CSV);
    }

    public void willReturnNullOnUnknownMediaType() {
        Assert.assertNull(MantaDataFileType.valueByMediaType("text/plain"));
    }
}
