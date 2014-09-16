package me.geso.tinyorm;

import static org.junit.Assert.*;

import java.beans.PropertyDescriptor;
import java.util.List;

import lombok.Data;
import me.geso.tinyorm.TableMeta;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;

import org.junit.Test;

public class TableMetaTest extends TestBase {

	@Test
	public void testPrimaryKeyMetas() {
		TableMeta tableMeta = orm.getTableMeta(Member.class);
		List<PropertyDescriptor> primaryKeyMetas = tableMeta.getPrimaryKeys();
		assertEquals(1, primaryKeyMetas.size());
		assertEquals("id", primaryKeyMetas.get(0).getName());
	}

	@Table("member")
	@Data
	public class Member {
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
