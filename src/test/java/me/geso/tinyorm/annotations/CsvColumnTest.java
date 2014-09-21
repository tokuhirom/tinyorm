package me.geso.tinyorm.annotations;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;

import org.junit.Before;
import org.junit.Test;

public class CsvColumnTest extends TestBase {
	@Before
	public void beefffff() throws SQLException {
		Connection connection = this.connection;
		connection.prepareStatement("DROP TABLE IF EXISTS foo").executeUpdate();
		connection
				.prepareStatement(
						"CREATE TABLE foo (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, csvInt TEXT NOT NULL, csvString TEXT NOT NULL)")
				.executeUpdate();
	}

	@Test
	public void testUpdateWithStatement() throws SQLException, RichSQLException {
		List<Integer> ints = Arrays.asList(5963, 4649);
		List<String> strings = Arrays.asList("John", "Manjiro");
		Foo foo = this.orm.insert(Foo.class).value("csvInt", ints)
				.value("csvString", strings).executeSelect();
		System.out.println(foo);
		assertEquals(foo.getCsvInt().size(), 2);
		assertEquals(foo.getCsvInt().get(0), (Integer) 5963);
		assertEquals(foo.getCsvInt().get(1), (Integer) 4649);
		assertEquals(foo.getCsvString().size(), 2);
		assertEquals(foo.getCsvString().get(0), "John");
		assertEquals(foo.getCsvString().get(1), "Manjiro");

		{
			List<Integer> ints2 = Arrays.asList(123, 456);
			List<String> strings2 = Arrays.asList("h,oge", "fuga");
			foo.update()
					.set("csvInt", ints2)
					.set("csvString", strings2)
					.execute();
			assertEquals(foo.refetch().get().getCsvInt().get(0).intValue(),
					123);
			assertEquals(foo.refetch().get().getCsvInt().get(1).intValue(),
					456);
			assertEquals(foo.refetch().get().getCsvString().get(0),
					"h,oge");
			assertEquals(foo.refetch().get().getCsvString().get(1),
					"fuga");
		}
	}

	@Table("foo")
	@Getter
	@Setter
	@ToString
	public static class Foo extends Row<Foo> {
		@PrimaryKey
		private long id;
		@CsvColumn
		private List<Integer> csvInt;
		@CsvColumn
		private List<String> csvString;
	}

}
