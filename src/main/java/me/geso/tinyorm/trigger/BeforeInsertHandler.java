package me.geso.tinyorm.trigger;

import me.geso.tinyorm.InsertStatement;
import me.geso.tinyorm.Row;

/**
 * Callback object. It will call back before call INSERT statement.
 */
@FunctionalInterface
public interface BeforeInsertHandler {
	public void callBeforeInsertHandler(InsertStatement<?> stmt);
}
