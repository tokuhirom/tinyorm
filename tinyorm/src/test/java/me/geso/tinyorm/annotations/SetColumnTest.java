package me.geso.tinyorm.annotations;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;

import org.junit.Test;

public class SetColumnTest extends TestBase {
	@Test
	public void test() {
		createTable(
			"foo",
			"vals SET('xxx', 'yyy')"
		);

		Set<String> values = new HashSet<>();
		values.add("xxx");
		values.add("yyy");
		orm.insert(Foo.class)
			.value("vals", values)
			.execute();

		List<Foo> foos = orm.search(Foo.class)
			.execute();
		assertEquals(1, foos.size());

		Set<String> expected = new HashSet<>();
		expected.add("xxx");
		expected.add("yyy");
		assertEquals(expected, foos.get(0).getVals());
	}

	@Table("foo")
	public static class Foo extends Row<Foo> {
		@SetColumn
		private Set<String> vals;

		public Set<String> getVals() {
			return vals;
		}

		public void setVals(Set<String> vals) {
			this.vals = vals;
		}
	}

}
