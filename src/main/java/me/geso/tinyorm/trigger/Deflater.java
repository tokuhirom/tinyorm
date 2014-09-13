package me.geso.tinyorm.trigger;

@FunctionalInterface
public interface Deflater {
	public Object deflate(String columnName, Object value); 
}
