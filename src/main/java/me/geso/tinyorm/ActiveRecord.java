package me.geso.tinyorm;

import java.util.Optional;

public abstract class ActiveRecord<T extends ActiveRecord<?>> implements
		ORMInjectable {
	private TinyORM orm;

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

	public void updateByBean(Object bean) {
		checkORM();
		this.orm.updateByBean(this, bean);
	}

	public UpdateRowStatement createUpdateStatement() {
		checkORM();
		return this.orm.createUpdateStatement(this);
	}

	public void delete() {
		checkORM();
		this.orm.delete(this);
	}
}
