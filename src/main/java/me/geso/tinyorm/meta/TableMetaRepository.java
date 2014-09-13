package me.geso.tinyorm.meta;

import java.util.concurrent.ConcurrentHashMap;

import me.geso.tinyorm.Row;

// Internal class. Do not use directly.
public class TableMetaRepository {
	private static ConcurrentHashMap<Class<? extends Row>, TableMeta> cache = new ConcurrentHashMap<>();

	public static TableMeta get(Class<? extends Row> klass) {
		TableMeta meta = cache.get(klass);
		if (meta == null) {
			meta = TableMeta.build(klass);
			cache.put(klass, meta);
		}
		return meta;
	}
	
}
