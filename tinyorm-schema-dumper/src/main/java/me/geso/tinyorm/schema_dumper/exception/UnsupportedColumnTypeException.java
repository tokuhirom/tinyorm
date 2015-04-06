package me.geso.tinyorm.schema_dumper.exception;

import me.geso.dbinspector.Column;

public class UnsupportedColumnTypeException extends Throwable {
	private Column column;

	public UnsupportedColumnTypeException(final Column column) {
		super(column.getName() + " has unsupported column type: " + column.getTypeName());
		this.column = column;
	}

	public Column getColumn() {
		return column;
	}
}
