package me.geso.tinyorm;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import me.geso.tinyorm.meta.TableMeta;
import me.geso.tinyorm.meta.TableMetaRepository;

/**
 * <pre>
 * {
 * 	&#064;code
 * 	class Foo extends BasicRow&lt;Foo&gt; {
 * 	}
 * }
 * </pre>
 *
 * @author Tokuhiro Matsuno
 * @param <Impl>
 *            The implementation class.
 */
public abstract class BasicRow<Impl extends Row> implements Row {

	private Connection connection;

	/**
	 * Set connection to row object. Normally, you don't need to use this
	 * method.
	 */
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	/**
	 * Get connection from row object.
	 */
	protected Connection getConnection() {
		if (this.connection == null) {
			throw new RuntimeException(
					"This row object doesn't have a connection information.");
		}
		return this.connection;
	}

	/**
	 * Get a where clause that selects the row from table. This method throws
	 * exception if the row doesn't have a primary key.
	 */
	public Query where() {
		Map<String, Object> pkmap = TableMetaRepository.get(this.getClass())
				.getPrimaryKeyValueMap(this);
		if (pkmap.isEmpty()) {
			throw new RuntimeException(
					"You can't delete row, doesn't have a primary keys.");
		}

		String sql = pkmap.keySet().stream().map(it
				-> "(" + quoteIdentifier(it) + "=?)"
				).collect(Collectors.joining(" AND "));
		List<Object> vars = pkmap.values().stream()
				.collect(Collectors.toList());
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
				throw new TinyORMException("Primary key should not be null: "
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
			if ((value instanceof Integer && (((Integer) value) == 0))
					|| (value instanceof Long && (((Long) value) == 0))
					|| (value instanceof Short && (((Short) value) == 0))) {
				throw new TinyORMException("Primary key should not be zero: "
						+ value);
			}
		}
	}

	public void delete() {
		try {
			TableMeta tableMeta = TableMetaRepository.get(this.getClass());
			String tableName = tableMeta.getName();
			Query where = where();

			StringBuilder buf = new StringBuilder();
			buf.append("DELETE FROM ").append(quoteIdentifier(tableName))
					.append(" WHERE ");
			buf.append(where.getSQL());
			String sql = buf.toString();

			int updated = TinyORM.prepare(connection, sql, where.getValues())
					.executeUpdate();
			if (updated != 1) {
				throw new RuntimeException("Cannot delete row: " + sql + " "
						+ where.getValues());
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Update row's properties by bean. And send UPDATE statement to the server.
	 * 
	 * @param bean
	 */
	public void updateByBean(Object bean) {
		TableMeta tableMeta = TableMetaRepository.get(this.getClass());
		Map<String, Object> currentValueMap = tableMeta.getColumnValueMap(this);

		try {
			UpdateRowStatement stmt = new UpdateRowStatement(this,
					this.getConnection(), this.getTableName());
			BeanInfo beanInfo = Introspector.getBeanInfo(bean.getClass(), Object.class);
			PropertyDescriptor[] propertyDescriptors = beanInfo
					.getPropertyDescriptors();
			final Method DEFLATE = this.getClass().getMethod("DEFLATE",
					String.class, Object.class);
			for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
				String name = propertyDescriptor.getName();
				if ("class".equals(name)) {
					continue;
				}
				if (!currentValueMap.containsKey(name)) {
					continue;
				}

				Object current = currentValueMap.get(name);
				Object newval = propertyDescriptor.getReadMethod().invoke(bean);
				if (newval != null) {
					if (!newval.equals(current)) {
						Object deflated = DEFLATE.invoke(
								this.getClass(), name,
								newval);
						stmt.set(name, deflated);
						tableMeta.setValue(this, name, newval);
					}
				} else { // newval IS NULL.
					if (current != null) {
						stmt.set(name, newval);
						tableMeta.setValue(this, name, newval);
					}
				}
			}
			if (!stmt.hasSetClause()) {
				return; // There is no updates.
			}
			this.FILL_UPDATED_TIMESTAMP(stmt);
			this.BEFORE_UPDATE(stmt);
			stmt.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Here is a hook point when updating row.
	 * 
	 * @param stmt
	 */
	public void BEFORE_UPDATE(UpdateRowStatement stmt) {
	}

	/**
	 * Set epoch time if there is the field named "updatedOn".
	 * 
	 * @param stmt
	 */
	public void FILL_UPDATED_TIMESTAMP(UpdateRowStatement stmt) {
		Field[] fields = this.getClass().getFields();
		for (Field field : fields) {
			if ("updatedOn".equals(field.getName())) {
				stmt.set("updatedOn", System.currentTimeMillis() / 1000);
			}
		}
	}

	private String quoteIdentifier(String identifier) {
		return TinyORM.quoteIdentifier(identifier, this.getConnection());
	}

	/**
	 * Fetch the latest row data from database.
	 * 
	 * @return
	 */
	public Optional<Impl> refetch() {
		Query where = this.where();

		StringBuilder buf = new StringBuilder();
		buf.append("SELECT * FROM ").append(
				quoteIdentifier(this.getTableName()));
		buf.append(" WHERE ").append(where.getSQL());
		String sql = buf.toString();

		try {
			Connection connection = this.getConnection();
			Object[] params = where.getValues();
			ResultSet rs = TinyORM.prepare(connection, sql, params)
					.executeQuery();
			if (rs.next()) {
				@SuppressWarnings("unchecked")
				Impl row = TinyORM.mapResultSet((Class<Impl>) this.getClass(),
						rs, connection);
				return Optional.of(row);
			} else {
				return Optional.empty();
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Get table name from the instance.
	 */
	protected String getTableName() {
		return TableMetaRepository.get(this.getClass()).getName();
	}

	public static Object INFLATE(String column, Object value) {
		return value;
	}

	public static Object DEFLATE(String column, Object value) {
		return value;
	}

}
