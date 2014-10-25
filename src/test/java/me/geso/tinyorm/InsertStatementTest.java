package me.geso.tinyorm;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import java.util.Optional;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.Table;

import org.junit.Before;
import org.junit.Test;

public class InsertStatementTest extends TestBase {

	@Before
	public void beforeeee() throws SQLException {
		orm.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS x")
				.executeUpdate();
		orm.getConnection()
				.prepareStatement(
						"CREATE TABLE x (a VARCHAR(255) NOT NULL, n int default 0, PRIMARY KEY (a))")
				.executeUpdate();

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
	public static class X extends Row<X> {
		@Column
		private String a;
		@Column
		private int n;
	}
}
