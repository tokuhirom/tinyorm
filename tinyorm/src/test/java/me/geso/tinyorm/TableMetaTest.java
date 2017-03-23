package me.geso.tinyorm;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;
import me.geso.tinyorm.exception.ConstructorIllegalArgumentException;

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
		assertTrue(tableMeta.hasColumn("q_no"));
		assertTrue(tableMeta.hasColumn("url"));
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
		assertThat(parameterNames, is(new String[]{"id", "name", "e_mail", "q_no", "url", "createdOn", "updatedOn"}));
	}

	@Table("member")
	@Value
	@EqualsAndHashCode(callSuper = false)
	private static class Member extends Row<Member> {
		@PrimaryKey
		private long id;
		@Column
		private String name;
		@Column("e_mail")
		private String email;
		@Column("q_no")
		private String qNo;
		@Column("url")
		private String URL;

		@CreatedTimestampColumn
		private long createdOn;
		@UpdatedTimestampColumn
		private long updatedOn;
	}

	/**
	 * ConstructorRowBuilder throw ConstructorException if constructor throws exception.
	 * That contains the cause of exception.
	 */
	@Test
	public void testConstructorRowBuilderException() throws Exception {
		// Scenario: wrong number of arguments
		createTable("member",
				"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
				"name VARCHAR(255) NOT NULL",
				"createdOn BIGINT",
				"updatedOn BIGINT");
		try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO member (id,name,createdOn) VALUES (1,'hoge',3)")) {
			preparedStatement.executeUpdate();
		}
		final TinyORM tinyORM = new TinyORM(connection);

		boolean thrown = false;
		try {
			final Optional<Member> memberOptional = tinyORM.single(Member.class)
					.where("id=?", 1)
					.execute();
		} catch (ConstructorIllegalArgumentException e) {
			assertThat(e.getMessage(), containsString("me.geso.tinyorm.TableMetaTest$Member#updatedOn(long) is not nullable."));
			thrown = true;
		}
		assertTrue(thrown);
	}

	/**
	 * This is a class for testing "member class" detection.
	 */
	@Table("member")
	@Value
	@EqualsAndHashCode(callSuper = false)
	private class NonStaticMember extends Row<Member> {
		@PrimaryKey
		private long id;
	}

	@Test
	public void testMemberClassDetection() throws Exception {
		final TinyORM tinyORM = new TinyORM(connection);
		boolean thrown = false;
		try {
			tinyORM.getTableName(NonStaticMember.class);
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(),
				containsString("Row class must not be non-static inner class"));
			thrown = true;
		}
		Assert.assertTrue(thrown);

		// Nomal Declaring Row Class.
		tinyORM.getTableName(OuterMember.class);
	}

}
