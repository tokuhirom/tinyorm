package me.geso.tinyorm.meta;

import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.trigger.BeforeUpdateHandler;
import me.geso.tinyorm.trigger.Deflater;
import me.geso.tinyorm.trigger.Inflater;

@Slf4j
public class DBSchema {
	private static ConcurrentHashMap<Class<? extends Row>, TableMeta> registry = new ConcurrentHashMap<>();

	public void registerClass(Class<? extends Row> klass) {
		TableMeta meta = TableMeta.build(klass);
		log.info("Loaded " + klass);
		registry.put(klass, meta);
	}

	public void addBeforeUpdateHandler(Class<? extends Row> klass,
			BeforeUpdateHandler handler) {
		TableMeta tableMeta = this.getTableMeta(klass);
		tableMeta.addBeforeUpdateHandler(handler);
	}

	public void addDeflater(Class<? extends Row> klass, Deflater deflater) {
		TableMeta tableMeta = this.getTableMeta(klass);
		tableMeta.addDeflater(deflater);
	}

	public void addInflater(Class<? extends Row> klass, Inflater inflater) {
		TableMeta tableMeta = this.getTableMeta(klass);
		tableMeta.addInflater(inflater);
	}

	public TableMeta getTableMeta(Class<? extends Row> klass) {
		TableMeta meta = registry.get(klass);
		if (meta == null) {
			log.info("Loading " + klass);
			meta = TableMeta.build(klass);
			registry.put(klass, meta);
		}
		return meta;
	}

}
