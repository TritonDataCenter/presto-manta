/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.google.common.io.Files;
import com.joyent.manta.presto.MantaPrestoFileType;
import com.joyent.manta.presto.exceptions.MantaPrestoIllegalArgumentException;
import org.apache.commons.lang3.StringUtils;

import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaPrestoColumnHandleFactory {
    public MantaPrestoColumnHandle getInstance(final String objectPath) {
        requireNonNull(objectPath, "Object path is null");

        if (StringUtils.isBlank(objectPath)) {
            String msg = "Object path is blank";
            MantaPrestoIllegalArgumentException e = new MantaPrestoIllegalArgumentException(msg);
            e.setContextValue("objectPathInBrackets", String.format("[%s]", objectPath));
            throw e;
        }

        final String extension = Files.getFileExtension(objectPath);

        if (StringUtils.isBlank(extension)) {
            String msg = "Object extension is blank";
            MantaPrestoIllegalArgumentException e = new MantaPrestoIllegalArgumentException(msg);
            e.setContextValue("objectPathExtensionInBrackets",
                    String.format("[%s]", extension));
            throw e;
        }

        MantaPrestoFileType type = MantaPrestoFileType.valueByExtension(extension);

        return null;
    }
}
