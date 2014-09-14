package me.geso.tinyorm;

import java.sql.Connection;

import me.geso.tinyorm.meta.TableMeta;

public interface Row {
	public void setConnection(Connection connection);
	public void setTableMeta(TableMeta tableMeta);
	public void setBeanMapper(BeanMapper beanMapper);
	public Query where();
}
