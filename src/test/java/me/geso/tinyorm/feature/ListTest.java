package me.geso.tinyorm.feature;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import me.geso.tinyorm.BasicRow;
import me.geso.tinyorm.TestBase;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.Deflate;
import me.geso.tinyorm.annotations.Inflate;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.meta.DBSchema;

import org.junit.Before;
import org.junit.Test;

public class ListTest extends TestBase {

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
	public void testFoo() throws SQLException {
		DBSchema schema = new DBSchema();
		schema.loadClass(Foo.class);

		List<Integer> ints = Arrays.asList(5963, 4649);
		List<String> strings = Arrays.asList("John", "Manjiro");
		Foo foo = this.orm.insert(Foo.class).value("csvInt", ints)
				.value("csvString", strings).executeSelect();
		assertEquals(foo.getCsvInt().size(), 2);
		assertEquals(foo.getCsvInt().get(0), (Integer) 5963);
		assertEquals(foo.getCsvInt().get(1), (Integer) 4649);
		assertEquals(foo.getCsvString().size(), 2);
		assertEquals(foo.getCsvString().get(0), "John");
		assertEquals(foo.getCsvString().get(1), "Manjiro");

		// updateByBean
		{
			FooUpdateForm fooUpdateForm = new FooUpdateForm();
			List<Integer> ints2 = Arrays.asList(123, 456);
			fooUpdateForm.setCsvInt(ints2);
			foo.updateByBean(fooUpdateForm);
			assertEquals(foo.refetch().get().getCsvInt().get(0).intValue(), 123);
		}

		// updateByBean(no UPDATE query required. Because the data set is same)
		{
			FooUpdateForm fooUpdateForm = new FooUpdateForm();
			List<Integer> ints2 = Arrays.asList(123, 456); // same as previous
			fooUpdateForm.setCsvInt(ints2);
			foo.updateByBean(fooUpdateForm);
			assertEquals(foo.refetch().get().getCsvInt().get(0).intValue(), 123);
		}
	}

	public static class FooUpdateForm {
		private List<Integer> csvInt;

		public List<Integer> getCsvInt() {
			return csvInt;
		}

		public void setCsvInt(List<Integer> csvInt) {
			this.csvInt = csvInt;
		}
	}

	@Table("foo")
	public static class Foo extends BasicRow<Foo> {
		@PrimaryKey
		private long id;
		@Column
		private List<Integer> csvInt;
		@Column
		private List<String> csvString;

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public List<Integer> getCsvInt() {
			return csvInt;
		}

		public void setCsvInt(List<Integer> csvInt) {
			this.csvInt = csvInt;
		}

		public List<String> getCsvString() {
			return csvString;
		}

		public void setCsvString(List<String> csvString) {
			this.csvString = csvString;
		}

		@Inflate("csvString")
		public static Object inflateCsvString(String value) {
			return Arrays.asList((value).split(","));
		}

		@Inflate("csvInt")
		public static Object inflateCsvInt(String value) {
			String[] split = value.split(",");
			return Arrays.stream(split).map(it -> Integer.parseInt(it))
					.collect(Collectors.toList());
		}

		@Deflate("csvInt")
		public static Object deflateCsvInt(List<Integer> value) {
			List<Integer> list = (List<Integer>) value;
			String string = list.stream().map(it -> it.toString())
					.collect(Collectors.joining(","));
			return string;
		}

		@Deflate("csvString")
		public static Object deflateCsvString(List<String> value) {
			List<String> list = (List<String>) value;
			return list.stream().collect(Collectors.joining(","));
		}
	}
}
