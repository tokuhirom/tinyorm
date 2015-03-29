package me.geso.tinyorm;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.Table;

public class InsertStatementTest extends TestBase {

	@Before
	public void beforeHook() throws SQLException {
		createTable("x",
			"a VARCHAR(255) NOT NULL",
			"n int default 0",
			"PRIMARY KEY (a)");
	}

	@Test
	public void test() {
		{
			assertThat(orm.insert(X.class).value("a", "hoge")
				.onDuplicateKeyUpdate("n=n+1")
				.execute(), is(1));
			Optional<X> row = orm.single(X.class).where("a=?", "hoge")
				.execute();
			assertThat(row.get().getN(), is(0));
		}
		{
			assertThat(orm.insert(X.class).value("a", "hoge")
				.onDuplicateKeyUpdate("n=n+1")
				.execute(), is(2));
			Optional<X> row = orm.single(X.class).where("a=?", "hoge")
				.execute();
			assertThat(row.get().getN(), is(1));
		}
		{
			assertThat(orm.insert(X.class).value("a", "hoge")
				.onDuplicateKeyUpdate("n=n+1")
				.execute(), is(2));
			Optional<X> row = orm.single(X.class).where("a=?", "hoge")
				.execute();
			assertThat(row.get().getN(), is(2));
		}
	}

	@Table("x")
	@Value
	@EqualsAndHashCode(callSuper = false)
	private static class X extends Row<X> {
		@Column
		private String a;
		@Column
		private int n;
	}
}
