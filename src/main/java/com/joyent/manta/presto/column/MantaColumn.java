/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.column;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 *
 */
public class MantaColumn extends ColumnMetadata implements ColumnHandle {
    @JsonCreator
    public MantaColumn(@JsonProperty("name") final String name,
                       @JsonProperty("type") final Type type,
                       @JsonProperty("extraInfo") final String extraInfo) {
        super(name, type, null, extraInfo, false);
    }

    @JsonProperty
    @Override
    public String getName() {
        return super.getName();
    }

    @JsonProperty
    @Override
    public Type getType() {
        return super.getType();
    }

    @JsonProperty
    @Override
    public String getComment() {
        return super.getComment();
    }

    @JsonProperty
    @Override
    public String getExtraInfo() {
        return super.getExtraInfo();
    }

    @Override
    public boolean isHidden() {
        return super.isHidden();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .toString();
    }
}
