package me.geso.tinyorm.meta;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import me.geso.tinyorm.InsertStatement;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.UpdateRowStatement;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedEpochTimestamp;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedEpochTimestamp;
import me.geso.tinyorm.trigger.BeforeInsertHandler;
import me.geso.tinyorm.trigger.BeforeUpdateHandler;

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

	TableMeta(String name, List<PrimaryKeyMeta> primaryKeyMetas,
			List<ColumnMeta> columnMetas,
			Map<String, PropertyDescriptor> propertyDescriptorMap,
			List<BeforeInsertHandler> beforeInsertTriggers,
			List<BeforeUpdateHandler> beforeUpdateTriggers) {
		this.name = name;
		this.primaryKeyMetas = primaryKeyMetas;
		this.columnMetas = columnMetas;
		this.propertyDescriptorMap = propertyDescriptorMap;
		this.beforeInsertHandlers = beforeInsertTriggers;
		this.beforeUpdateHandlers = beforeUpdateTriggers;
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
			if (field.getAnnotation(CreatedEpochTimestamp.class) != null) {
				beforeInsertTriggers.add(new CreatedEpochTimestampColumnHook(
						field.getName()));
				isColumn = true;
			}
			if (field.getAnnotation(UpdatedEpochTimestamp.class) != null) {
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

		String tableName = TableMeta.getTableName(rowClass);

		return new TableMeta(tableName, primaryKeys, columns,
				propertyDescriptorMap, beforeInsertTriggers,
				beforeUpdateTriggers);
	}

	/**
	 * This method may not thread safe.
	 */
	public void addBeforeInsertHandler(BeforeInsertHandler handler) {
		this.beforeInsertHandlers.add(handler);
	}

	/**
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
		} catch (NullPointerException| IllegalAccessException
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
}
