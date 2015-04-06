package me.geso.tinyorm.schema_dumper.exception;

import lombok.Getter;

public class UnknownTableException extends Exception {
	@Getter
	private String tableName;

	public UnknownTableException(final String tableName) {
		super("There is no table named: '" + tableName + "'");
		this.tableName = tableName;
	}
}
