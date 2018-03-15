/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.types;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.SqlTimestamp;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

/**
 * An epoc seconds timestamp is stored as seconds from 1970-01-01T00:00:00 UTC.
 * When performing calculations on a timestamp the client's time zone must be
 * taken into account.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 1.0.0
 */
public final class TimestampEpochSecondsType extends AbstractLongType {
    /**
     * Plaintext representation of type.
     */
    public static final String TYPE_SIGNATURE_STRING = "timestamp epoch seconds";

    /**
     * Singleton instance of type.
     */
    public static final TimestampEpochSecondsType TIMESTAMP_EPOCH_SECONDS =
            new TimestampEpochSecondsType();

    private TimestampEpochSecondsType() {
        super(parseTypeSignature(TYPE_SIGNATURE_STRING));
    }

    @Override
    public Object getObjectValue(final ConnectorSession session,
                                 final Block block,
                                 final int position) {
        if (block.isNull(position)) {
            return null;
        }

        final long numericValue = block.getLong(position, 0);
        final long millisecondValue = numericValue * 1_000L;

        return new SqlTimestamp(millisecondValue, session.getTimeZoneKey());
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object other) {
        return other == TIMESTAMP_EPOCH_SECONDS;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
