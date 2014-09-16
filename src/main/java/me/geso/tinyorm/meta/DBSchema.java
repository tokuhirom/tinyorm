package me.geso.tinyorm.meta;

import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DBSchema {
	private static ConcurrentHashMap<Class<?>, TableMeta> registry = new ConcurrentHashMap<>();

	public TableMeta getTableMeta(Class<?> klass) {
		TableMeta meta = registry.get(klass);
		if (meta == null) {
			log.info("Loading {}", klass);
			meta = TableMeta.build(klass);
			registry.put(klass, meta);
		}
		return meta;
	}

}
