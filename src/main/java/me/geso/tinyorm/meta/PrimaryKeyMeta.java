package me.geso.tinyorm.meta;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

public class PrimaryKeyMeta extends ColumnMeta {

	public PrimaryKeyMeta(String name, Method readMethod, Method writeMethod) {
		super(name, readMethod, writeMethod);
	}

	public static PrimaryKeyMeta build(PropertyDescriptor propertyDescriptor) {
		String name = propertyDescriptor.getName();
		Method readMethod = propertyDescriptor.getReadMethod();
		Method writeMethod = propertyDescriptor.getWriteMethod();
		return new PrimaryKeyMeta(name, readMethod, writeMethod);
	}

}
