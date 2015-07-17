package me.geso.tinyorm.deflate;

import java.util.Set;
import java.util.stream.Collectors;

import me.geso.tinyorm.trigger.Deflater;

/**
 * Convert List&gt;String&lt; to String
 */
public class SetDeflater implements Deflater {
	@Override
	public Object deflate(final Object value) {
		if (value == null) {
			return null;
		}

		if (value instanceof Set<?>) {
			return ((Set<String>)value).stream()
				.collect(Collectors.joining(","));
		} else {
			throw new IllegalArgumentException("SetDeflater supports only Set");
		}
	}
}
