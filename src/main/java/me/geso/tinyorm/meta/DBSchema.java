package me.geso.tinyorm.meta;

import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;
import me.geso.tinyorm.Row;

@Slf4j
public class DBSchema {
	private static ConcurrentHashMap<Class<? extends Row>, TableMeta> registry = new ConcurrentHashMap<>();

	public void loadClass(Class<? extends Row> klass) {
		TableMeta meta = TableMeta.build(klass);
		log.info("Loaded " + klass);
		registry.put(klass, meta);
	}

	public TableMeta getTableMeta(Class<? extends Row> klass) {
		TableMeta meta = registry.get(klass);
		if (meta == null) {
			loadClass(klass);
			meta = registry.get(klass);
		}
		return meta;
	}

}
