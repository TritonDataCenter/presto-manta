/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.facebook.presto.spi.ColumnMetadata;
import com.joyent.manta.presto.MantaPrestoFileType;

import java.util.List;

/**
 *
 */
public interface ColumnLister {
    List<MantaPrestoColumn> listColumns(final String objectPath,
                                        final MantaPrestoFileType type,
                                        final String firstLine);
}
