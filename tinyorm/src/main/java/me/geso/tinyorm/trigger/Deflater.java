package me.geso.tinyorm.trigger;

@FunctionalInterface
public interface Deflater {
	public Object deflate(Object value);
}
