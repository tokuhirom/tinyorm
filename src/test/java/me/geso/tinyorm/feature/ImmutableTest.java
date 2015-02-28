package me.geso.tinyorm.feature;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

public class ImmutableTest extends TestBase {
	@Before
	public void beefffff() throws SQLException, RichSQLException {
		this.orm.updateBySQL("DROP TABLE IF EXISTS foo");
		this.orm.updateBySQL("CREATE TABLE foo (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))");
	}

	@Test
	public void testFoo() throws SQLException, RichSQLException {
		Foo foo = this.orm.insert(Foo.class)
			.value("name", "John")
			.executeSelect();
		assertThat(foo.getName(), is("John"));
	}

	@Table("foo")
	@ToString
	@AllArgsConstructor
	@Getter
	public static class Foo extends Row<Foo> {
		@PrimaryKey
		private long id;
		@Column
		private String name;
	}
}
