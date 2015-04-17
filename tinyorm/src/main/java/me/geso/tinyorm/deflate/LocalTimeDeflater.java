package me.geso.tinyorm.deflate;

import me.geso.tinyorm.trigger.Deflater;

public class LocalTimeDeflater implements Deflater {
	@Override
	public Object deflate(final Object value) {
		if (value == null) {
			return null;
		}

		if (value instanceof java.time.LocalTime) {
			return java.sql.Time.valueOf((java.time.LocalTime)value);
		} else {
			throw new IllegalArgumentException("LocalTimeDeflater supports only java.time.LocalDate");
		}
	}
}
