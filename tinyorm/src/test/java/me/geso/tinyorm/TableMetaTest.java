package me.geso.tinyorm;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.List;

import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;

public class TableMetaTest extends TestBase {

	@Test
	public void testPrimaryKeyMetas() {
		TableMeta<?> tableMeta = orm
			.getTableMeta(Member.class);
		List<PropertyDescriptor> primaryKeyMetas = tableMeta.getPrimaryKeys();
		assertEquals(1, primaryKeyMetas.size());
		assertEquals("id", primaryKeyMetas.get(0).getName());
	}

	@Test
	public void testHasColumn() {
		TableMeta<?> tableMeta = orm
			.getTableMeta(Member.class);
		assertTrue(tableMeta.hasColumn("id"));
		assertFalse(tableMeta.hasColumn("unknown"));
	}

	@Test
	public void testAlias() throws IllegalAccessException, NoSuchFieldException {
		TableMeta<?> tableMeta = orm
				.getTableMeta(Member.class);
		final Field rowBuilderField = TableMeta.class.getDeclaredField("rowBuilder");
		rowBuilderField.setAccessible(true);
		final Object rowBuilder = rowBuilderField.get(tableMeta);
		final Field parameterNamesField = rowBuilder.getClass().getDeclaredField("parameterNames");
		parameterNamesField.setAccessible(true);
		final String[] parameterNames = (String[])parameterNamesField.get(rowBuilder);
		assertThat(parameterNames, is(new String[] {"id", "name", "e_mail", "createdOn", "updatedOn" }));
	}

	@Table("member")
	@Value
	@EqualsAndHashCode(callSuper = false)
	private class Member extends Row<Member> {
		@PrimaryKey
		private long id;
		@Column
		private String name;
		@Column("e_mail")
		private String email;

		@CreatedTimestampColumn
		private long createdOn;
		@UpdatedTimestampColumn
		private long updatedOn;
	}
}
