package me.geso.tinyorm.deflate;

import java.time.LocalDate;

import me.geso.tinyorm.trigger.Deflater;

/**
 * LocalDate -> String
 */
public class LocalDateDeflater implements Deflater {
	@Override
	public Object deflate(final Object value) {
		if (value == null) {
			return null;
		}

		if (value instanceof java.time.LocalDate) {
			return java.sql.Date.valueOf((LocalDate)value);
		} else {
			throw new IllegalArgumentException("LocalDateDeflater supports only java.time.LocalDate");
		}
	}
}
