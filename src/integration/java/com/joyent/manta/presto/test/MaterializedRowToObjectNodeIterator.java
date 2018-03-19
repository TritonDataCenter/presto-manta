/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.presto.test;


import com.facebook.presto.testing.MaterializedRow;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;

public class MaterializedRowToObjectNodeIterator implements Iterator<ObjectNode>,
        Iterable<ObjectNode> {
    private Iterator<MaterializedRow> inner;
    private final List<String> columns;

    public MaterializedRowToObjectNodeIterator(final List<String> columns,
            final Iterator<MaterializedRow> inner) {
        this.columns = columns;
        this.inner = inner;
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public ObjectNode next() {
        MaterializedRow row = inner.next();

        if (row == null) {
            return null;
        }

        ObjectNode objectNode = new ObjectNode(JsonNodeFactory.instance);

        for (int i = 0; i < row.getFieldCount(); i++) {
            final Object field = row.getField(i);
            final String column = columns.get(i);

            writeJsonForType(objectNode, field, column);
        }

        return objectNode;
    }

    @Override
    public Iterator<ObjectNode> iterator() {
        return this;
    }

    private static void writeJsonForType(final ObjectNode node, Object field,
                                         final String column) {
        if (field.getClass().equals(int.class)) {
            node.put(column, (int)field);
        } else if (field.getClass().equals(Integer.class)) {
            node.put(column, (Integer)field);
        } else if (field.getClass().equals(long.class)) {
            node.put(column, (long)field);
        } else if (field.getClass().equals(Long.class)) {
            node.put(column, (Long)field);
        } else if (field.getClass().equals(short.class)) {
            node.put(column, (short)field);
        } else if (field.getClass().equals(short.class)) {
            node.put(column, (Short)field);
        } else if (field.getClass().equals(float.class)) {
            node.put(column, (float)field);
        } else if (field.getClass().equals(Float.class)) {
            node.put(column, (Float)field);
        } else if (field.getClass().equals(double.class)) {
            node.put(column, (double)field);
        } else if (field.getClass().equals(Double.class)) {
            node.put(column, (Double)field);
        } else if (field.getClass().equals(boolean.class)) {
            node.put(column, (boolean)field);
        } else if (field.getClass().equals(Boolean.class)) {
            node.put(column, (Boolean)field);
        } else if (field.getClass().equals(BigDecimal.class)) {
            node.put(column, (BigDecimal)field);
        } else if (field.getClass().equals(String.class)) {
            node.put(column, (String)field);
        } else if (field.getClass().equals(java.sql.Date.class)) {
            final long epoch = ((java.sql.Date)field).getTime();
            final Instant instant = Instant.ofEpochMilli(epoch);
            final LocalDate date = LocalDateTime.ofInstant(
                    instant, ZoneOffset.UTC).toLocalDate();

            node.putPOJO(column,  date.toString());
        } else if (field.getClass().equals(java.time.LocalDate.class)) {
            final LocalDate localDate = (LocalDate)field;

            node.put(column, localDate.toEpochDay());
        } else {
            node.putPOJO(column, field);
        }
    }
}
