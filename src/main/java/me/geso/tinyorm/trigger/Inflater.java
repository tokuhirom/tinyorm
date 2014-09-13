package me.geso.tinyorm.trigger;

@FunctionalInterface
public interface Inflater {
	public Object inflate(String columnName, Object value); 
}
