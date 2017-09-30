/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

/**
 * A singleton transaction handle used internally by Presto.
 *
 * @since 1.0.0
 */
public enum MantaTransactionHandle implements ConnectorTransactionHandle {
    /**
     * Single global instance.
     */
    INSTANCE;
}
