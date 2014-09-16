package me.geso.tinyorm;

/**
 * If there is an extra columns, I can eat extra columns.<br>
 * 
 * @see ActiveRecord.setExtraColumn
 */
public interface ExtraColumnSettable {
	public void setExtraColumn(String name, Object value);
}
