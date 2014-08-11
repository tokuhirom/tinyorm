package me.geso.tinyorm;

import java.sql.Connection;

public interface Row {
	public Connection getConnection();
	public void setConnection(Connection connection);
	public Query where();
	public String getTableName();
}
