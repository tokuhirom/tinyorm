package me.geso.tinyorm;

import java.beans.BeanInfo;
import java.beans.ConstructorProperties;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import me.geso.jdbcutils.JDBCUtils;
import me.geso.jdbcutils.Query;
import me.geso.tinyorm.annotations.BeforeInsert;
import me.geso.tinyorm.annotations.BeforeUpdate;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.CsvColumn;
import me.geso.tinyorm.annotations.Deflate;
import me.geso.tinyorm.annotations.Inflate;
import me.geso.tinyorm.annotations.JsonColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;
import me.geso.tinyorm.trigger.BeforeInsertHandler;
import me.geso.tinyorm.trigger.BeforeUpdateHandler;
import me.geso.tinyorm.trigger.Deflater;
import me.geso.tinyorm.trigger.Inflater;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TableMeta<RowType extends Row<?>> {
	private final String name;
	private final List<PropertyDescriptor> primaryKeys;
	// columnName -> propertyDescriptor
	private final Map<String, PropertyDescriptor> propertyDescriptorMap;
	private final List<BeforeInsertHandler> beforeInsertHandlers;
	private final List<BeforeUpdateHandler> beforeUpdateHandlers;
	private final Map<String, Inflater> inflaters;
	private final Map<String, Deflater> deflaters;
	private final RowBuilder rowBuilder;

	TableMeta(String name, List<PropertyDescriptor> primaryKeyMetas,
			Map<String, PropertyDescriptor> propertyDescriptorMap,
			List<BeforeInsertHandler> beforeInsertTriggers,
			List<BeforeUpdateHandler> beforeUpdateTriggers,
			Map<String, Inflater> inflaters, Map<String, Deflater> deflaters,
			RowBuilder rowBuilder) {
		this.name = name;
		this.primaryKeys = primaryKeyMetas;
		this.propertyDescriptorMap = propertyDescriptorMap;
		this.beforeInsertHandlers = beforeInsertTriggers;
		this.beforeUpdateHandlers = beforeUpdateTriggers;
		this.inflaters = inflaters;
		this.deflaters = deflaters;
		this.rowBuilder = rowBuilder;
	}

	static <RowType extends Row<?>> TableMeta<RowType> build(
			Class<RowType> rowClass)
			throws IntrospectionException {
		BeanInfo beanInfo = Introspector.getBeanInfo(rowClass, Object.class);
		PropertyDescriptor[] propertyDescriptors = beanInfo
			.getPropertyDescriptors();
		List<PropertyDescriptor> primaryKeys = new ArrayList<>();
		List<BeforeInsertHandler> beforeInsertTriggers = new ArrayList<>();
		List<BeforeUpdateHandler> beforeUpdateTriggers = new ArrayList<>();
		Map<String, PropertyDescriptor> propertyDescriptorMap = new LinkedHashMap<>();
		// It should be stable... I want to use LinkedHashMap here.
		final Map<String, Inflater> inflaters = new LinkedHashMap<>();
		final Map<String, Deflater> deflaters = new LinkedHashMap<>();
		List<Field> fields = new ArrayList<>();
		Collections.addAll(fields, rowClass.getDeclaredFields());
		Class<?> superClass = rowClass.getSuperclass();
		while (!superClass.isAssignableFrom(Row.class)) {
			Collections.addAll(fields, superClass.getDeclaredFields());
			superClass = superClass.getSuperclass();
		}
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
				primaryKeys.add(propertyDescriptor);
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
			if (field.getAnnotation(JsonColumn.class) != null) {
				// deserialize json
				Type type = field.getGenericType();
				JavaType javaType = TypeFactory.defaultInstance()
					.constructType(type);
				JsonInflater inflater = new JsonInflater(
					rowClass, propertyDescriptor, javaType);
				inflaters.put(propertyDescriptor.getName(), inflater);
				// serialize json
				JsonDeflater deflater = new JsonDeflater(
					rowClass, propertyDescriptor, javaType);
				deflaters.put(propertyDescriptor.getName(), deflater);
				isColumn = true;
			}
			if (field.getAnnotation(CsvColumn.class) != null) {
				// deserialize json
				if (!Collection.class.isAssignableFrom(field.getType())) {
					throw new RuntimeException(
						"You can't add @CsvColumn annotation for non-Collection field.");
				}
				Type type = field.getGenericType();
				if (type instanceof ParameterizedType) {
					Type[] actualTypeArguments = ((ParameterizedType)type)
						.getActualTypeArguments();
					if (actualTypeArguments.length != 1) {
						throw new RuntimeException(
							"You can only use List<String>, List<Integer> for @CsvColumn.");
					}
					Type actualTypeArgument = actualTypeArguments[0];
					Class<?> klass = null;
					if (actualTypeArgument instanceof Class) {
						if (Integer.class
							.isAssignableFrom((Class<?>)actualTypeArgument)) {
							klass = Integer.class;
						} else if (String.class
							.isAssignableFrom((Class<?>)actualTypeArgument)) {
							klass = String.class;
						} else {
							throw new RuntimeException(
								"You can only use List<String>, List<Integer> for @CsvColumn.");
						}
						CsvInflater inflater = new CsvInflater(
							klass);
						inflaters.put(propertyDescriptor.getName(), inflater);
						// serialize json
						CsvDeflater deflater = new CsvDeflater();
						deflaters.put(propertyDescriptor.getName(), deflater);
						isColumn = true;
					} else {
						throw new RuntimeException(
							"@CsvColumn should be List<String> or List<Integer>.");
					}

				} else {
					throw new RuntimeException(field.getName()
						+ " field isn't generic type.");
				}
			}

			if (isColumn) {
				propertyDescriptorMap.put(propertyDescriptor.getName(),
					propertyDescriptor);
			}
		}

		// Scanning hooks.
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
							"Duplicated @Inflate in %s(%s).",
							rowClass.getName(), columnName));
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
							"Duplicated @Deflate in %s(%s).",
							rowClass.getName(), columnName));
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

		// Checking constructor
		RowBuilder rowBuilder = buildRowBuilder(rowClass);

		String tableName = TableMeta.getTableName(rowClass);

		return new TableMeta<RowType>(tableName, primaryKeys,
			propertyDescriptorMap, beforeInsertTriggers,
			beforeUpdateTriggers, inflaters, deflaters,
			rowBuilder);
	}

	private static <T extends Row<?>> RowBuilder buildRowBuilder(
			Class<?> rowClass) {
		for (Constructor<?> constructor : rowClass.getConstructors()) {
			ConstructorProperties annotation = constructor
				.getAnnotation(java.beans.ConstructorProperties.class);
			if (annotation != null) {
				return new ConstructorRowBuilder<T>(constructor,
					annotation.value());
			}
		}
		return new SetterRowBuilder();
	}

	/**
	 * Add before insert handler<br>
	 * This method may not thread safe.
	 */
	void addBeforeInsertHandler(BeforeInsertHandler handler) {
		this.beforeInsertHandlers.add(handler);
	}

	/**
	 * Add before update handler<br>
	 * This method may not thread safe.
	 *
	 * @param handler
	 */
	void addBeforeUpdateHandler(BeforeUpdateHandler handler) {
		this.beforeUpdateHandlers.add(handler);
	}

	// Internal use.
	private static String getTableName(Class<?> rowClass) {
		Table table = rowClass.getAnnotation(Table.class);
		if (table == null) {
			throw new RuntimeException("Missing @Table annotation");
		}
		String tableName = table.value();
		return tableName;
	}

	Object getValue(Object row, String columnName) {
		try {
			PropertyDescriptor propertyDescriptor = this.propertyDescriptorMap
				.get(columnName);
			if (propertyDescriptor == null) {
				throw new RuntimeException("Unknown column: " + columnName
					+ " in " + this.getName());
			}
			Method readMethod = propertyDescriptor.getReadMethod();
			Object value = readMethod.invoke(row);
			return value;
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	// Internal use.
	Map<String, Object> getColumnValueMap(Object row) {
		Map<String, Object> map = new LinkedHashMap<>(); // I guess it should be
															// ordered.
		try {
			for (PropertyDescriptor propertyDescriptor : this.propertyDescriptorMap
				.values()) {
				Method readMethod = propertyDescriptor.getReadMethod();
				Object value = readMethod.invoke(row);
				map.put(propertyDescriptor.getName(), value);
			}
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new RuntimeException(e);
		}
		return map;
	}

	// Internal use.
	Map<String, Object> getPrimaryKeyValueMap(Object row) {
		Map<String, Object> map = new LinkedHashMap<>(); // I guess it should be
															// ordered.
		try {
			for (PropertyDescriptor pk : this.getPrimaryKeys()) {
				Method readMethod = pk.getReadMethod();
				Object value = readMethod.invoke(row);
				map.put(pk.getName(), value);
			}
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw new RuntimeException(e);
		}
		return map;
	}

	/**
	 * Get a where clause that selects the row from table. This method throws
	 * exception if the row doesn't have a primary key.
	 */
	Query createWhereClauseFromRow(Object row, String identifierQuoteString) {
		Map<String, Object> pkmap = this.getPrimaryKeyValueMap(row);
		if (pkmap.isEmpty()) {
			throw new RuntimeException(
				"You can't delete row, doesn't have a primary keys.");
		}

		String sql = pkmap
			.keySet()
			.stream()
			.map(it
				-> "("
					+ JDBCUtils.quoteIdentifier(it,
						identifierQuoteString) + "=?)"
			).collect(Collectors.joining(" AND "));
		List<Object> vars = new ArrayList<>(pkmap.values());
		this.validatePrimaryKeysForSelect(vars);
		return new Query(sql, vars);
	}

	/**
	 * This method validates primary keys for SELECT row from the table. You can
	 * override this method.
	 *
	 * If you detected primary key constraints violation, you can throw the
	 * RuntimeException.
	 */
	protected void validatePrimaryKeysForSelect(List<Object> values) {
		for (Object value : values) {
			if (value == null) {
				throw new RuntimeException("Primary key should not be null: "
					+ this);
			}
		}

		/*
		 * 0 is a valid value for primary key. But, normally, it's just a bug.
		 * If you want to use 0 as a primary key value, please overwrite this
		 * method.
		 */
		if (values.size() == 1) {
			Object value = values.get(0);
			if ((value instanceof Integer && (((Integer)value) == 0))
				|| (value instanceof Long && (((Long)value) == 0))
				|| (value instanceof Short && (((Short)value) == 0))) {
				throw new RuntimeException("Primary key should not be zero: "
					+ value);
			}
		}
	}

	void invokeBeforeInsertTriggers(InsertStatement<?> stmt) {
		for (BeforeInsertHandler trigger : this.beforeInsertHandlers) {
			trigger.callBeforeInsertHandler(stmt);
		}
	}

	void invokeBeforeUpdateTriggers(UpdateRowStatement<?> stmt) {
		for (BeforeUpdateHandler trigger : this.beforeUpdateHandlers) {
			trigger.callBeforeUpdateHandler(stmt);
		}
	}

	Object invokeInflater(String columnName, Object value) {
		Inflater inflater = this.inflaters.get(columnName);
		if (inflater != null) {
			return inflater.inflate(value);
		} else {
			return value;
		}
	}

	Object invokeDeflater(String columnName, Object value) {
		Deflater deflater = this.deflaters.get(columnName);
		if (deflater != null) {
			return deflater.deflate(value);
		} else {
			return value;
		}
	}

	public String getName() {
		return name;
	}

	public List<PropertyDescriptor> getPrimaryKeys() {
		return primaryKeys;
	}

	public boolean hasColumn(String columnName) {
		return propertyDescriptorMap.containsKey(columnName);
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
		public void callBeforeUpdateHandler(UpdateRowStatement<?> stmt) {
			stmt.set(this.columnName, System.currentTimeMillis() / 1000);
		}

	}

	@ToString
	static class BeforeInsertMethodTrigger implements BeforeInsertHandler {
		private final Method method;
		private final Class<?> rowClass;

		BeforeInsertMethodTrigger(final Class<?> rowClass,
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
		private final Class<?> rowClass;

		BeforeUpdateMethodTrigger(final Class<?> rowClass,
				final Method method) {
			this.method = method;
			this.rowClass = rowClass;
		}

		@Override
		public void callBeforeUpdateHandler(UpdateRowStatement<?> stmt) {
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
		private final Class<?> rowClass;

		public MethodInflater(Class<?> rowClass, Method method) {
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
		private final Class<?> rowClass;

		public MethodDeflater(Class<?> rowClass,
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
				log.info("Can't invoke deflation method: {} with {}",
					method.toGenericString(), value.getClass());
				throw new RuntimeException(e);
			}
		}
	}

	@ToString
	static class JsonInflater implements Inflater {
		private final Class<?> rowClass;
		private final PropertyDescriptor propertyDescriptor;
		private final JavaType javaType;
		private final ObjectMapper mapper = new ObjectMapper();

		JsonInflater(Class<?> rowClass,
				PropertyDescriptor propertyDescriptor, JavaType javaType) {
			this.rowClass = rowClass;
			this.propertyDescriptor = propertyDescriptor;
			this.javaType = javaType;
		}

		@Override
		public Object inflate(Object value) {
			if (value instanceof byte[]) {
				try {
					return mapper.readValue((byte[])value, javaType);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else {
				throw new RuntimeException(
					"You shouldn't apply @JsonColumn for non byte[].");
			}
		}
	}

	@ToString
	static class JsonDeflater implements Deflater {
		private final Class<?> rowClass;
		private final PropertyDescriptor propertyDescriptor;
		private final JavaType javaType;
		private final ObjectMapper mapper = new ObjectMapper();

		JsonDeflater(Class<?> rowClass,
				PropertyDescriptor propertyDescriptor, JavaType javaType) {
			this.rowClass = rowClass;
			this.propertyDescriptor = propertyDescriptor;
			this.javaType = javaType;
		}

		@Override
		public Object deflate(Object value) {
			try {
				byte[] bytes = mapper.writeValueAsBytes(value);
				return bytes;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@ToString
	static class CsvInflater implements Inflater {
		private final Class<?> klass;

		public CsvInflater(Class<?> klass) {
			this.klass = klass;
		}

		@Override
		public Object inflate(Object value) {
			if (value == null) {
				return null;
			}

			if (value instanceof String) {
				try {
					try (CSVParser parse = CSVParser.parse((String)value,
						CSVFormat.RFC4180)) {
						for (CSVRecord record : parse.getRecords()) {
							if (this.klass == String.class) {
								List<String> list = new ArrayList<>();
								for (String column : record) {
									list.add(column);
								}
								return Collections.unmodifiableList(list);
							} else if (this.klass == Integer.class) {
								List<Integer> list = new ArrayList<>();
								for (String column : record) {
									list.add(Integer.parseInt(column));
								}
								return Collections.unmodifiableList(list);
							} else {
								throw new RuntimeException(
									"Should not reache here.");
							}
						}
						return null;
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else {
				throw new RuntimeException(
					"You shouldn't apply @CsvColumn for non String.");
			}
		}
	}

	@ToString
	static class CsvDeflater implements Deflater {
		@Override
		public Object deflate(Object value) {
			try {
				if (value instanceof Iterable) {
					StringBuilder builder = new StringBuilder();
					try (final CSVPrinter printer = new CSVPrinter(builder,
						CSVFormat.RFC4180)) {
						printer.printRecord((Iterable<?>)value);
						String csv = builder.toString();
						if (csv.endsWith("\n")) {
							return csv.substring(0, csv.length() - 2);
						} else {
							return csv;
						}
					}
				} else {
					throw new RuntimeException(
						"@CsvColumn must be Iterable but "
							+ value.getClass());
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	RowType createRowFromResultSet(final Class<RowType> klass,
			final ResultSet rs,
			final TinyORM orm) throws SQLException {
		return this.rowBuilder.<RowType>build((Class<RowType>)klass, this,
			rs, orm);
	}

	private static interface RowBuilder {
		public <RowType extends Row<?>> RowType build(
				final Class<RowType> klass,
				final TableMeta<RowType> tableMeta,
				final ResultSet rs, final TinyORM orm)
				throws SQLException;
	}

	private static class ConstructorRowBuilder<T extends Row<?>> implements
			RowBuilder {
		private final Constructor<?> constructor;
		private final String[] parameterNames;
		private final Map<String, Integer> parameterPositionFor;

		public ConstructorRowBuilder(Constructor<?> constructor,
				String[] parameterNames) {
			this.constructor = constructor;
			this.parameterNames = parameterNames;
			this.parameterPositionFor = new HashMap<>();
			for (int i = 0; i < parameterNames.length; ++i) {
				this.parameterPositionFor.put(parameterNames[i], i);
			}
		}

		@Override
		public <RowType extends Row<?>> RowType build(
				final Class<RowType> klass,
				final TableMeta<RowType> tableMeta,
				final ResultSet rs, final TinyORM orm)
				throws SQLException {
			Object[] initargs = new Object[parameterNames.length];
			int columnCount = rs.getMetaData().getColumnCount();
			Map<String, Object> extraColumns = new HashMap<>();
			ResultSetMetaData metaData = rs.getMetaData();
			for (int i = 0; i < columnCount; ++i) {
				String columnName = metaData.getColumnLabel(i + 1);
				Object value = rs.getObject(i + 1);
				value = tableMeta.invokeInflater(columnName, value);
				Integer idx = parameterPositionFor.get(columnName);
				if (idx != null) {
					initargs[idx] = value;
				} else {
					extraColumns.put(columnName, value);
				}
			}
			try {
				@SuppressWarnings("unchecked")
				RowType row = (RowType)constructor.newInstance(initargs);
				for (Entry<String, Object> entry : extraColumns.entrySet()) {
					row.setExtraColumn(entry.getKey(), entry.getValue());
				}
				row.setOrm(orm);
				return row;
			} catch (InstantiationException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException e) {
				log.error(
					"{}: {}, {}, extraColumns:{}, parameterPositionFor:{}, {}",
					e.getClass(), klass,
					Arrays.toString(initargs), extraColumns,
					parameterPositionFor, e.getMessage());
				throw new RuntimeException(e);
			}
		}
	}

	private static class SetterRowBuilder implements
			RowBuilder {
		@Override
		public <T extends Row<?>> T build(Class<T> klass,
				TableMeta<T> tableMeta,
				ResultSet rs,
				TinyORM orm) throws SQLException {
			try {
				int columnCount = rs.getMetaData().getColumnCount();
				T row = klass.newInstance();
				for (int i = 0; i < columnCount; ++i) {
					String columnName = rs.getMetaData().getColumnLabel(i + 1);
					Object value = rs.getObject(i + 1);
					value = tableMeta.invokeInflater(columnName, value);
					this.setValue(tableMeta, row, columnName, value);
				}
				row.setOrm(orm);
				return row;
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		// Internal use.
		private <T extends Row<?>> void setValue(TableMeta<T> tableMeta,
				Row<?> row,
				String columnName, Object value) {
			PropertyDescriptor propertyDescriptor = tableMeta.propertyDescriptorMap
				.get(columnName);
			if (propertyDescriptor != null) {
				Method writeMethod = propertyDescriptor.getWriteMethod();
				if (writeMethod != null) {
					try {
						writeMethod.invoke(row, value);
					} catch (NullPointerException | IllegalAccessException
							| IllegalArgumentException
							| InvocationTargetException e) {
						log.error(
							"Error:{}: table: {}, column: {}, writeMethod:{}, valueClass:{}, value:{}, row:{}",
							e.getClass(),
							tableMeta.getName(),
							columnName,
							writeMethod.getName(),
							value == null ? null : value.getClass(),
							value,
							row.toString()
							);
						throw new RuntimeException(e);
					}
				} else {
					throw new RuntimeException(String.format(
						"There is no writer method: %s.%s",
						tableMeta.getName(),
						propertyDescriptor.getName()));
				}
			} else {
				System.out.println("HAHAHA");
				row.setExtraColumn(columnName, value);
			}
		}

	}
}
