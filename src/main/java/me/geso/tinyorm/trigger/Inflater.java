package me.geso.tinyorm.trigger;

/**
 * This class is to inflate the object.<br>
 * Example: You can implement classes that return Java object from the data that
 * has been serialized in JSON format.
 */
@FunctionalInterface
public interface Inflater {
	public Object inflate(Object value);
}
