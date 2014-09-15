package me.geso.tinyorm.meta;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import me.geso.tinyorm.InsertStatement;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.UpdateRowStatement;
import me.geso.tinyorm.annotations.BeforeInsert;
import me.geso.tinyorm.annotations.BeforeUpdate;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.Deflate;
import me.geso.tinyorm.annotations.Inflate;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;
import me.geso.tinyorm.trigger.BeforeInsertHandler;
import me.geso.tinyorm.trigger.BeforeUpdateHandler;
import me.geso.tinyorm.trigger.Deflater;
import me.geso.tinyorm.trigger.Inflater;

@Slf4j
public class TableMeta {
	@Getter
	private final String name;
	@Getter
	private final List<PrimaryKeyMeta> primaryKeyMetas;
	private final List<ColumnMeta> columnMetas;
	private final Map<String, PropertyDescriptor> propertyDescriptorMap;
	private final List<BeforeInsertHandler> beforeInsertHandlers;
	private final List<BeforeUpdateHandler> beforeUpdateHandlers;
	private final Map<String, Inflater> inflaters;
	private final Map<String, Deflater> deflaters;

	TableMeta(String name, List<PrimaryKeyMeta> primaryKeyMetas,
			List<ColumnMeta> columnMetas,
			Map<String, PropertyDescriptor> propertyDescriptorMap,
			List<BeforeInsertHandler> beforeInsertTriggers,
			List<BeforeUpdateHandler> beforeUpdateTriggers,
			Map<String, Inflater> inflaters, Map<String, Deflater> deflaters) {
		this.name = name;
		this.primaryKeyMetas = primaryKeyMetas;
		this.columnMetas = columnMetas;
		this.propertyDescriptorMap = propertyDescriptorMap;
		this.beforeInsertHandlers = beforeInsertTriggers;
		this.beforeUpdateHandlers = beforeUpdateTriggers;
		this.inflaters = inflaters;
		this.deflaters = deflaters;
	}

	// Internal use.
	@SneakyThrows
	public static TableMeta build(Class<? extends Row> rowClass) {
		BeanInfo beanInfo = Introspector.getBeanInfo(rowClass, Object.class);
		PropertyDescriptor[] propertyDescriptors = beanInfo
				.getPropertyDescriptors();
		List<PrimaryKeyMeta> primaryKeys = new ArrayList<>();
		List<ColumnMeta> columns = new ArrayList<>();
		List<BeforeInsertHandler> beforeInsertTriggers = new ArrayList<>();
		List<BeforeUpdateHandler> beforeUpdateTriggers = new ArrayList<>();
		Map<String, PropertyDescriptor> propertyDescriptorMap = new LinkedHashMap<>();
		Field[] fields = rowClass.getDeclaredFields();
		Map<String, Field> fieldMap = new HashMap<>();
		for (Field field : fields) {
			fieldMap.put(field.getName(), field);
		}
		for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
			String name = propertyDescriptor.getName();
			if ("class".equals(name) || "classLoader".equals(name)) {
				continue;
			}
			if (!fieldMap.containsKey(name)) {
				continue;
			}

			Field field = fieldMap.get(name);
			boolean isColumn = false;
			if (field.getAnnotation(PrimaryKey.class) != null) {
				primaryKeys.add(PrimaryKeyMeta.build(propertyDescriptor));
				isColumn = true;
			}
			if (field.getAnnotation(Column.class) != null) {
				isColumn = true;
			}
			if (field.getAnnotation(CreatedTimestampColumn.class) != null) {
				beforeInsertTriggers.add(new CreatedEpochTimestampColumnHook(
						field.getName()));
				isColumn = true;
			}
			if (field.getAnnotation(UpdatedTimestampColumn.class) != null) {
				beforeInsertTriggers.add(new UpdatedEpochTimestampColumnHook(
						field.getName()));
				beforeUpdateTriggers.add(new UpdatedEpochTimestampColumnHook(
						field.getName()));
				isColumn = true;
			}

			if (isColumn) {
				columns.add(ColumnMeta.build(propertyDescriptor));
				propertyDescriptorMap.put(propertyDescriptor.getName(),
						propertyDescriptor);
			}
		}

		// It should be stable... I want to use LinkedHashMap here.
		final Map<String, Inflater> inflaters = new LinkedHashMap<>();
		final Map<String, Deflater> deflaters = new LinkedHashMap<>();
		for (Method method : rowClass.getMethods()) {
			if (method.getAnnotation(BeforeInsert.class) != null) {
				beforeInsertTriggers.add(new BeforeInsertMethodTrigger(
						rowClass, method));
			}
			if (method.getAnnotation(BeforeUpdate.class) != null) {
				beforeUpdateTriggers.add(new BeforeUpdateMethodTrigger(
						rowClass, method));
			}
			{
				Inflate inflate = method.getAnnotation(Inflate.class);
				if (inflate != null) {
					String columnName = inflate.value();
					if (inflaters.containsKey(columnName)) {
						throw new RuntimeException(String.format(
								"Duplicated @Inflate in %s(%s).", rowClass.getName(), columnName));
					}
					if (Modifier.isStatic(method.getModifiers())) {
						inflaters.put(columnName, new MethodInflater(rowClass,
								method));
					} else {
						throw new RuntimeException(
								String.format(
										"%s.%s has a @Inflate annotation. But it's not a 'static' method. You should add 'static' modifier.",
										rowClass.getName(), method.getName()));
					}
				}
			}
			{
				Deflate deflate = method.getAnnotation(Deflate.class);
				if (deflate != null) {
					String columnName = deflate.value();
					if (deflaters.containsKey(columnName)) {
						throw new RuntimeException(String.format(
								"Duplicated @Deflate in %s(%s).", rowClass.getName(),  columnName));
					}
					if (Modifier.isStatic(method.getModifiers())) {
						deflaters.put(columnName, new MethodDeflater(rowClass,
								method));
					} else {
						throw new RuntimeException(
								String.format(
										"%s.%s has a @Deflate annotation. But it's not a 'static' method. You should add 'static' modifier.",
										rowClass.getName(), method.getName()));
					}
				}
			}
		}

		String tableName = TableMeta.getTableName(rowClass);

		return new TableMeta(tableName, primaryKeys, columns,
				propertyDescriptorMap, beforeInsertTriggers,
				beforeUpdateTriggers, inflaters, deflaters);
	}

	/**
	 * Add before insert handler<br>
	 * This method may not thread safe.
	 */
	public void addBeforeInsertHandler(BeforeInsertHandler handler) {
		this.beforeInsertHandlers.add(handler);
	}

	/**
	 * Add before update handler<br>
	 * This method may not thread safe.
	 * 
	 * @param handler
	 */
	public void addBeforeUpdateHandler(BeforeUpdateHandler handler) {
		this.beforeUpdateHandlers.add(handler);
	}

	// Internal use.
	private static String getTableName(Class<? extends Row> rowClass) {
		Table table = rowClass.getAnnotation(Table.class);
		if (table == null) {
			throw new RuntimeException("Missing @Table annotation");
		}
		String tableName = table.value();
		return tableName;
	}

	// Internal use.
	public Map<String, Object> getColumnValueMap(Row row) {
		Map<String, Object> map = new LinkedHashMap<>(); // I guess it should be
															// ordered.
		for (ColumnMeta columnMeta : this.columnMetas) {
			Object value = columnMeta.get(row);
			map.put(columnMeta.getName(), value);
		}
		return map;
	}

	// Internal use.
	public Map<String, Object> getPrimaryKeyValueMap(Row row) {
		Map<String, Object> map = new LinkedHashMap<>(); // I guess it should be
															// ordered.
		for (PrimaryKeyMeta pkMeta : this.primaryKeyMetas) {
			Object value = pkMeta.get(row);
			map.put(pkMeta.getName(), value);
		}
		return map;
	}

	// Internal use.
	public void setValue(Row row, String name, Object value) {
		PropertyDescriptor propertyDescriptor = this.propertyDescriptorMap
				.get(name);
		if (propertyDescriptor == null) {
			throw new RuntimeException(
					String.format(
							"setValue: %s doesn't have a %s column. You may forget to set @Column annotation.",
							row.getClass(), name
							));
		}
		Method writeMethod = propertyDescriptor.getWriteMethod();
		if (writeMethod == null) {
			throw new RuntimeException(String.format(
					"There is no writer method: %s.%s", this.getName(),
					propertyDescriptor.getName()));
		}
		try {
			writeMethod.invoke(row, value);
		} catch (NullPointerException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			log.error("{}: {}, {}, {}, {}, {}, valueClass:{}", e.getClass(),
					this.getName(),
					row, name, writeMethod.getName(), value,
					value == null ? null : value.getClass());
			throw new RuntimeException(e);
		}
	}

	// Ineternal use
	public void invokeBeforeInsertTriggers(InsertStatement<?> stmt) {
		for (BeforeInsertHandler trigger : this.beforeInsertHandlers) {
			trigger.callBeforeInsertHandler(stmt);
		}
	}

	// Ineternal use
	public void invokeBeforeUpdateTriggers(UpdateRowStatement stmt) {
		for (BeforeUpdateHandler trigger : this.beforeUpdateHandlers) {
			trigger.callBeforeUpdateHandler(stmt);
		}
	}

	// Ineternal use
	public Object invokeInflaters(String columnName, Object value) {
		Inflater inflater = this.inflaters.get(columnName);
		if (inflater != null) {
			return inflater.inflate(value);
		} else {
			return value;
		}
	}

	// Ineternal use
	public Object invokeDeflaters(String columnName, Object value) {
		Deflater deflater = this.deflaters.get(columnName);
		if (deflater != null) {
			return deflater.deflate(value);
		} else {
			return value;
		}
	}

	@ToString
	static class CreatedEpochTimestampColumnHook implements
			BeforeInsertHandler {
		private String columnName;

		public CreatedEpochTimestampColumnHook(String columnName) {
			this.columnName = columnName;
		}

		@Override
		public void callBeforeInsertHandler(InsertStatement<?> stmt) {
			stmt.value(this.columnName, System.currentTimeMillis() / 1000);
		}

	}

	@ToString
	static class UpdatedEpochTimestampColumnHook implements
			BeforeInsertHandler, BeforeUpdateHandler {
		private String columnName;

		public UpdatedEpochTimestampColumnHook(String columnName) {
			this.columnName = columnName;
		}

		@Override
		public void callBeforeInsertHandler(InsertStatement<?> stmt) {
			stmt.value(this.columnName, System.currentTimeMillis() / 1000);
		}

		@Override
		public void callBeforeUpdateHandler(UpdateRowStatement stmt) {
			stmt.set(this.columnName, System.currentTimeMillis() / 1000);
		}

	}

	@ToString
	static class BeforeInsertMethodTrigger implements BeforeInsertHandler {
		private final Method method;
		private final Class<? extends Row> rowClass;

		BeforeInsertMethodTrigger(final Class<? extends Row> rowClass,
				final Method method) {
			this.method = method;
			this.rowClass = rowClass;
		}

		@Override
		public void callBeforeInsertHandler(InsertStatement<?> stmt) {
			try {
				method.invoke(rowClass, stmt);
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@ToString
	static class BeforeUpdateMethodTrigger implements BeforeUpdateHandler {
		private final Method method;
		private final Class<? extends Row> rowClass;

		BeforeUpdateMethodTrigger(final Class<? extends Row> rowClass,
				final Method method) {
			this.method = method;
			this.rowClass = rowClass;
		}

		@Override
		public void callBeforeUpdateHandler(UpdateRowStatement stmt) {
			try {
				method.invoke(rowClass, stmt);
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Slf4j
	@ToString
	static class MethodInflater implements Inflater {
		private final Method method;
		private final Class<? extends Row> rowClass;

		public MethodInflater(Class<? extends Row> rowClass, Method method) {
			this.rowClass = rowClass;
			this.method = method;
		}

		@Override
		public Object inflate(Object value) {
			try {
				return this.method.invoke(rowClass, value);
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				log.info("Can't invoke inflation method: {}",
						method.toGenericString());
				throw new RuntimeException(e);
			}
		}

	}

	@ToString
	static class MethodDeflater implements Deflater {
		private final Method method;
		private final Class<? extends Row> rowClass;

		public MethodDeflater(Class<? extends Row> rowClass,
				@NonNull Method method) {
			this.rowClass = rowClass;
			this.method = method;
		}

		@Override
		public Object deflate(Object value) {
			try {
				return this.method.invoke(this.rowClass, value);
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				log.info("Can't invoke deflation method: {}",
						method.toGenericString());
				throw new RuntimeException(e);
			}
		}

	}
}
