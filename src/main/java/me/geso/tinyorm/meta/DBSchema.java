package me.geso.tinyorm.meta;

import java.util.concurrent.ConcurrentHashMap;

import me.geso.tinyorm.Row;

public class DBSchema {
	private static ConcurrentHashMap<Class<? extends Row>, TableMeta> registry = new ConcurrentHashMap<>();
	
	public void registerClass(Class<? extends Row> klass) {
		TableMeta meta = TableMeta.build(klass);
		registry.put(klass, meta);
	}

	public TableMeta getTableMeta(Class<? extends Row> klass) {
		TableMeta meta = registry.get(klass);
		if (meta == null) {
			throw new RuntimeException("Unknown row class: " + klass);
		}
		return meta;
	}
	
}
