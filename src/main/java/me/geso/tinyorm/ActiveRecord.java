package me.geso.tinyorm;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public abstract class ActiveRecord<T extends ActiveRecord<?>> implements
		ORMInjectable, ExtraColumnSettable {
	private TinyORM orm;
	private Map<String, Object> extraColumns = new LinkedHashMap<>();

	public void setOrm(TinyORM orm) {
		this.orm = orm;
	}

	private void checkORM() {
		if (this.orm == null) {
			throw new IllegalStateException(
					"This object doesn't have a ORM information");
		}
	}

	@SuppressWarnings("unchecked")
	public Optional<T> refetch() {
		checkORM();
		return this.orm.refetch((T) this);
	}

	public UpdateRowStatement update() {
		checkORM();
		return this.orm.createUpdateStatement(this);
	}

	public void delete() {
		checkORM();
		this.orm.delete(this);
	}
	
	@Override
	public void setExtraColumn(String columnName, Object value) {
		this.extraColumns.put(columnName, value);
	}
	
	public Object getExtraColumn(String columnName) {
		return this.extraColumns.get(columnName);
	}
}
