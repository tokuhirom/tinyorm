package me.geso.tinyorm;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.PropertyUtilsBean;

/**
 * <pre>
 * <code>
 * class Foo extends BasicRow<Foo> {
 * }
 * </code>
 * </pre>
 *
 * @author Tokuhiro Matsuno <tokuhirom@gmail.com>
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
		List<String> primaryKeys = TinyORM.getPrimaryKeys(this.getClass());
		if (primaryKeys.isEmpty()) {
			throw new RuntimeException(
					"You can't delete row, doesn't have a primary keys.");
		}

		String sql = primaryKeys.stream().map(it
				-> "(" + quoteIdentifier(it) + "=?)"
				).collect(Collectors.joining(" AND "));
		List<Object> vars = primaryKeys
				.stream()
				.map(pk -> {
					try {
						Object value = BeanUtilsBean.getInstance()
								.getPropertyUtils().getProperty(this, pk);
						return value;
					} catch (IllegalArgumentException | IllegalAccessException
							| SecurityException | InvocationTargetException
							| NoSuchMethodException ex) {
						throw new RuntimeException(ex);
					}
				}).collect(Collectors.toList());
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
			if (value instanceof Long) {
				System.out.println(value.toString());
				long lvalue = (Long) value;
				if (lvalue == 0) {
					System.out.println("YAY");
				}
			}
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
			String table = TinyORM.getTableName(this.getClass());
			Query where = where();

			StringBuilder buf = new StringBuilder();
			buf.append("DELETE FROM ").append(quoteIdentifier(table))
					.append(" WHERE ");
			buf.append(where.getSQL());
			String sql = buf.toString();

			int updated = TinyORM.prepare(connection, sql, where.getValues()).executeUpdate();
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
		try {
			UpdateRowStatement stmt = new UpdateRowStatement(this, this.getConnection(), this.getTableName());
			PropertyUtilsBean propertyUtils = BeanUtilsBean.getInstance()
					.getPropertyUtils();
			PropertyDescriptor[] propertyDescriptors = propertyUtils
					.getPropertyDescriptors(bean);
			Arrays.stream(propertyDescriptors)
					.map(prop -> prop.getName())
					.filter(key -> !"class".equals(key))
					.forEach(
							name -> {
								try {
									Object current = propertyUtils.getProperty(
											this, name);
									Object newval = propertyUtils.getProperty(
											bean, name);
									if (newval != null) {
										if (!newval.equals(current)) {
											stmt.set(name, newval);
											propertyUtils.setProperty(this,
													name, newval);
										}
									} else {
										// newval IS NULL.
							if (current != null) {
								stmt.set(name, newval);
								propertyUtils.setProperty(this,
										name, newval);
							}
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}	);
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
			ResultSet rs = TinyORM.prepare(connection, sql, params).executeQuery();
			if (rs.next()) {
				@SuppressWarnings("unchecked")
				Impl row =TinyORM.mapResultSet((Class<Impl>)this.getClass(), rs, connection);
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
		return TinyORM.getTableName(this.getClass());
	}
	
	public static Object INFLATE(String column, Object value) {
		return value;
	}

	public static Object DEFLATE(String column, Object value) {
		return value;
	}

}
