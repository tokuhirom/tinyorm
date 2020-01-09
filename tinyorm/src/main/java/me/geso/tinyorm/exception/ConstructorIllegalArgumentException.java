package me.geso.tinyorm.exception;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public class ConstructorIllegalArgumentException extends RuntimeException {
	private static final Map<Class<?>, Class<?>> PRIMITIVES_TO_WRAPPERS = buildPrimitivesToWrappers();

	public ConstructorIllegalArgumentException(final IllegalArgumentException e, final Class<?> klass,
			final Constructor<?> constructor,
			final String[] parameterNames, final Object[] initargs) {
		super(buildMessage(klass, constructor, parameterNames, initargs), e);
	}

	private static String buildMessage(final Class<?> klass,
			final Constructor<?> constructor,
			final String[] parameterNames, final Object[] initargs) {
		final StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("Cannot create row object from DB data.\n");

		Class<?>[] parameterTypes = constructor.getParameterTypes();
		for (int i = 0; i < parameterTypes.length; ++i) {
			if (initargs[i] == null) {
				if (parameterTypes[i].isPrimitive()) {
					stringBuilder.append(klass.getName())
							.append("#")
							.append(parameterNames[i])
							.append("(")
							.append(parameterTypes[i].getName())
							.append(") is not nullable.\n");
				}
			} else {
				if (parameterTypes[i].isPrimitive()) {
					if (!wrap(parameterTypes[i]).isAssignableFrom(initargs[i].getClass())) {
						stringBuilder.append(klass.getName())
								.append("#")
								.append(parameterNames[i])
								.append("(")
								.append(parameterTypes[i].getName())
								.append(") is not assignable from")
								.append(initargs[i].getClass().getName())
								.append(".\n");
					}
				} else {
					if (!parameterTypes[i].isAssignableFrom(initargs[i].getClass())) {
						stringBuilder.append(klass.getName())
								.append("#")
								.append(parameterNames[i])
								.append("(")
								.append(parameterTypes[i].getName())
								.append(") is not assignable from")
								.append(initargs[i].getClass().getName())
								.append(".\n");
					}
				}
			}
		}
		return stringBuilder.toString();
	}

	private static Map<Class<?>, Class<?>> buildPrimitivesToWrappers() {
		Map<Class<?>, Class<?>> m = new HashMap<>();
		m.put(boolean.class, Boolean.class);
		m.put(byte.class, Byte.class);
		m.put(char.class, Character.class);
		m.put(double.class, Double.class);
		m.put(float.class, Float.class);
		m.put(int.class, Integer.class);
		m.put(long.class, Long.class);
		m.put(short.class, Short.class);
		m.put(void.class, Void.class);
		return m;
	}

	@SuppressWarnings("unchecked")
	private static <T> Class<T> wrap(Class<T> c) {
		return c.isPrimitive() ? (Class<T>)PRIMITIVES_TO_WRAPPERS.get(c) : c;
	}
}
