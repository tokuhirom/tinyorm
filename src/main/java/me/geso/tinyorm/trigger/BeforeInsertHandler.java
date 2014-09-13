package me.geso.tinyorm.trigger;

import me.geso.tinyorm.InsertStatement;

/**
 * Callback object. It will call back before call INSERT statement.
 */
@FunctionalInterface
public interface BeforeInsertHandler {
	public void callBeforeInsertHandler(InsertStatement<?> stmt);
}
