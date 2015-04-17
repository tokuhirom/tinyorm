package me.geso.tinyorm.inflate;

import java.sql.Time;

import me.geso.tinyorm.trigger.Inflater;

public class LocalTimeInflater implements Inflater {
	@Override
	public Object inflate(final Object value) {
		if (value == null) {
			return null;
		}

		if (value instanceof java.sql.Time) {
			return ((java.sql.Time)value).toLocalTime();
		} else {
			throw new IllegalArgumentException("LocalTimeInflater doesn't support " + value.getClass());
		}
	}
}

