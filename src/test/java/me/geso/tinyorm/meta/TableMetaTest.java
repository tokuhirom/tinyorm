package me.geso.tinyorm.meta;

import static org.junit.Assert.*;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import me.geso.tinyorm.BasicRow;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;
import me.geso.tinyorm.meta.PrimaryKeyMeta;
import me.geso.tinyorm.meta.TableMeta;
import me.geso.tinyorm.meta.DBSchema;

import org.junit.Test;

public class TableMetaTest {

	@Test
	public void testPrimaryKeyMetas() {
		DBSchema schema = new DBSchema();
		TableMeta tableMeta = schema.getTableMeta(Member.class);
		List<PrimaryKeyMeta> primaryKeyMetas = tableMeta.getPrimaryKeyMetas();
		assertEquals(1, primaryKeyMetas.size());
		assertEquals("id", primaryKeyMetas.get(0).getName());
	}

	@Table("member")
	@Data
	@EqualsAndHashCode(callSuper = false)
	public class Member extends BasicRow<Member> {
		@PrimaryKey
		private long id;
		@Column
		private String name;

		@CreatedTimestampColumn
		private long createdOn;
		@UpdatedTimestampColumn
		private long updatedOn;
	}
}
