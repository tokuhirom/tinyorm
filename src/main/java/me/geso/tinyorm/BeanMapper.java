package me.geso.tinyorm;

import java.sql.Connection;
import java.sql.ResultSet;

public interface BeanMapper {
	public <T extends Row> T mapResultSet(Class<T> klass, ResultSet rs,
			Connection connection);
}
