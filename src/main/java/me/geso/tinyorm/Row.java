package me.geso.tinyorm;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public abstract class Row<T extends Row<?>> {
	private final Map<String, Object> extraColumns = new LinkedHashMap<>();
	private TinyORM orm;

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
	 * @return Fetched object. It may return empty response if other connection deleted the row.
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
	 * @return Created UpdateRowStatement object.
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
	 * <pre>
	 *     <code>
	 *         List<Entry> entries = orm.searchBySQL(EntryRow.class, "SELECT entry.*, blog.title blogTitle
	 *                           FROM entry INNER JOIN blog ON (entry.blogId=blog.id)");
	 *         for (Entry entry: entries) {
	 *             System.out.println(String.format("%s %s",
	 *                 entry.getExtraColumn("blogTitle), entry.getTitle());
	 *         }
	 *     </code>
	 * </pre>
	 *
	 * @param columnName column name to get
	 * @return The value for columnName.
	 */
	public Object getExtraColumn(String columnName) {
		return this.extraColumns.get(columnName);
	}

	/**
	 * Get extra columns.
	 *
	 * @return Get all extra columns in map.
	 */
	public Map<String, Object> getExtraColumns() {
		return Collections.unmodifiableMap(this.extraColumns);
	}

}
