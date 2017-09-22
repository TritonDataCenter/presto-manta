/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.joyent.manta.presto.MantaDataFileType;

import java.util.List;

/**
 *
 */
public interface ColumnLister {
    List<MantaColumn> listColumns(final String objectPath,
                                  final MantaDataFileType type,
                                  final String firstLine);
}
