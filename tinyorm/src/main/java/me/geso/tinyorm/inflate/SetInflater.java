package me.geso.tinyorm.inflate;

import java.util.HashSet;
import java.util.Set;

import me.geso.tinyorm.trigger.Inflater;

public class SetInflater implements Inflater {
	@Override
	public Object inflate(final Object value) {
		if (value == null) {
			return null;
		}

		if (value instanceof String) {
			Set<String> result = new HashSet<>();
			for (String s: ((String)value).split(",")) {
				result.add(s);
			}
			return result;
		} else {
			throw new IllegalArgumentException("SetInflater doesn't support " + value.getClass());
		}
	}
}

