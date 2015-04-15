package me.geso.tinyorm.inflate;

import java.sql.Date;

import me.geso.tinyorm.trigger.Inflater;

public class LocalDateInflater implements Inflater {
	@Override
	public Object inflate(final Object value) {
		if (value == null) {
			return null;
		}

		if (value instanceof java.sql.Date) {
			return ((Date)value).toLocalDate();
		} else {
			throw new IllegalArgumentException("LocalDateInflater doesn't support " + value.getClass());
		}
	}
}
