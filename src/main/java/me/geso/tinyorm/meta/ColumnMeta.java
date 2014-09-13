package me.geso.tinyorm.meta;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

import lombok.Getter;
import lombok.SneakyThrows;
import me.geso.tinyorm.Row;

public class ColumnMeta {

	private final Method writeMethod;
	private final Method readMethod;
	@Getter
	private final String name;

	public ColumnMeta(String name, Method readMethod, Method writeMethod) {
		this.name = name;
		this.readMethod = readMethod;
		this.writeMethod = writeMethod;
	}

	public static ColumnMeta build(PropertyDescriptor propertyDescriptor) {
		String name = propertyDescriptor.getName();
		Method readMethod = propertyDescriptor.getReadMethod();
		Method writeMethod = propertyDescriptor.getWriteMethod();
		return new ColumnMeta(name, readMethod, writeMethod);
	}
	

	@SneakyThrows
	public Object get(Row row) {
		return this.readMethod.invoke(row);
	}

	@SneakyThrows
	public void set(Row row, Object value) {
		this.writeMethod.invoke(row, value);
	}
}
