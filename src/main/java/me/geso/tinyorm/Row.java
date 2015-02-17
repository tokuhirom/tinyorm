package me.geso.tinyorm;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public abstract class Row<T extends Row<?>> {
	private TinyORM orm;
	private Map<String, Object> extraColumns = new LinkedHashMap<>();

	/**
	 * Internal API
	 */
	public void setOrm(TinyORM orm) {
		this.orm = orm;
	}

	private void checkORM() {
		if (this.orm == null) {
			throw new IllegalStateException(
				"This object doesn't have a ORM information");
		}
	}

	/**
	 * Fetch the latest row data from database.
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Optional<T> refetch() {
		checkORM();
		return this.orm.refetch((T)this);
	}

	/**
	 * Create {@code UpdateRowStatement} with where clause that selects this
	 * row.
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public UpdateRowStatement<T> update() {
		checkORM();
		return this.orm.createUpdateStatement((T)this);
	}

	/**
	 * Delete this line from the database.
	 */
	public void delete() {
		checkORM();
		this.orm.delete(this);
	}

	/**
	 * Internal API.
	 */
	public void setExtraColumn(String columnName, Object value) {
		this.extraColumns.put(columnName, value);
	}

	/**
	 * Get extra column value.
	 *
	 * @param columnName
	 * @return
	 */
	public Object getExtraColumn(String columnName) {
		return this.extraColumns.get(columnName);
	}

	/**
	 * Get extra columns.
	 *
	 * @return
	 */
	public Map<String, Object> getExtraColumns() {
		return Collections.unmodifiableMap(this.extraColumns);
	}

}
