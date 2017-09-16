/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.google.inject.Injector;
import com.joyent.manta.client.MantaClient;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class MantaPrestoModuleTest {
    private Injector injector;

    @BeforeClass
    public void before() {
        injector = MantaPrestoTestUtils.createInjectorInstance(
                MantaPrestoTestUtils.UNIT_TEST_CONFIG);
    }

    public void verifyMantaClientInstancesAreTheSameInstance() {
        MantaClient instance1 = injector.getInstance(MantaClient.class);
        MantaClient instance2 = injector.getInstance(MantaClient.class);

        Assert.assertSame(instance1, instance2,
                "Dependency injection is generating a different instance "
                        + "per getInstance() call");
    }
}
