package me.geso.tinyorm.meta;

import static org.junit.Assert.*;

import java.util.List;

import me.geso.tinyorm.Member;
import me.geso.tinyorm.meta.PrimaryKeyMeta;
import me.geso.tinyorm.meta.TableMeta;
import me.geso.tinyorm.meta.TableMetaRepository;

import org.junit.Test;

public class TableMetaTest {

	@Test
	public void testPrimaryKeyMetas() {
		TableMeta tableMeta = TableMetaRepository.get(Member.class);
		List<PrimaryKeyMeta> primaryKeyMetas = tableMeta.getPrimaryKeyMetas();
		assertEquals(1, primaryKeyMetas.size());
		assertEquals("id", primaryKeyMetas.get(0).getName());
	}

}
