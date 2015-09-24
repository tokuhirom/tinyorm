package me.geso.tinyorm.inflate;

import me.geso.tinyorm.trigger.Inflater;

public class LocalDateTimeInflater implements Inflater {
    @Override
    public Object inflate(final Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        } else {
            throw new IllegalArgumentException("LocalDateTimeInflater doesn't support " + value.getClass());
        }
    }
}
