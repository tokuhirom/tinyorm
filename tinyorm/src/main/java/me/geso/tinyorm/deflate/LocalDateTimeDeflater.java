package me.geso.tinyorm.deflate;

import me.geso.tinyorm.trigger.Deflater;

/**
 * Convert java.time.LocalDateTIme to java.sql.Timestamp.
 */
public class LocalDateTimeDeflater implements Deflater {
    @Override
    public Object deflate(final Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof java.time.LocalDateTime) {
            return java.sql.Timestamp.valueOf((java.time.LocalDateTime) value);
        } else {
            throw new IllegalArgumentException("LocalDateTimeDeflater supports only java.time.LocalDateTime");
        }
    }
}
