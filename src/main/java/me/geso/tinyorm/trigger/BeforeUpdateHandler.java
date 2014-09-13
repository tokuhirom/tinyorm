package me.geso.tinyorm.trigger;

import me.geso.tinyorm.UpdateRowStatement;

/**
 * Callback object. It will call back before call UPDATE statement.
 */
public interface BeforeUpdateHandler {

	public void callBeforeUpdateHandler(UpdateRowStatement stmt);
}
