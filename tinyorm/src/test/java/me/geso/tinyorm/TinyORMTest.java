package me.geso.tinyorm;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Provider;

import org.junit.Before;
import org.junit.Test;

import lombok.Data;
import lombok.EqualsAndHashCode;
import me.geso.jdbcutils.RichSQLException;
import me.geso.jdbcutils.UncheckedRichSQLException;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;

public class TinyORMTest extends TestBase {

	@Before
	public final void setupSchema() throws RichSQLException {
		createTable("member",
			"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
			"name VARCHAR(255)",
			"createdOn INT UNSIGNED DEFAULT NULL",
			"updatedOn INT UNSIGNED DEFAULT NULL");

		createTable("blog",
			"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
			, "memberId INT UNSIGNED NOT NULL"
			, "title VARCHAR(255)"
			, "createdOn INT UNSIGNED DEFAULT NULL"
			, "updatedOn INT UNSIGNED DEFAULT NULL");

		createTable("foo",
			"y integer",
			"z varchar(255)");

		createTable("bar",
			"y varchar(255)",
			"z varchar(255)");
	}

	@Test
	public void singleSimple() throws SQLException, RichSQLException {
		this.orm.updateBySQL("INSERT INTO member (name, createdOn, updatedOn) VALUES ('m1',1410581698,1410581698)");

		{
			Member got = this.orm.singleBySQL(Member.class, "SELECT * FROM member WHERE name=?",
					Arrays.asList("m1")).get();
			assertEquals(1, got.getId());
			assertEquals("m1", got.getName());
		}

		// Specified connection
		{
			Member got = this.orm.singleBySQL(Member.class, "SELECT * FROM member WHERE name=?", Arrays.asList("m1"),
					orm.getConnection()).get();
			assertEquals(1, got.getId());
			assertEquals("m1", got.getName());
		}
	}

	@Test
	public void testInsert() throws SQLException, InstantiationException,
			IllegalAccessException, RichSQLException {
		Member member = this.orm.insert(Member.class)
			.value("name", "John")
			.executeSelect();
		assertEquals(member.getName(), "John");
		assertEquals(member.getId(), 1);
		// assertNotEquals(0, member.getCreatedOn());
	}

	@Test
	public void insertByBean() throws SQLException, RichSQLException {
		MemberForm form = new MemberForm();
		form.setName("Nick");
		Member member = this.orm.insert(Member.class)
			.valueByBean(form)
			.executeSelect();
		assertEquals(member.getName(), "Nick");
		assertEquals(member.getId(), 1);
	}

	@SuppressWarnings({"unused", "UnusedAssignment"})
	@Test
	public void single() throws SQLException, RichSQLException {
		Member member1 = this.orm.insert(Member.class).value("name", "m1")
			.executeSelect();
		Member member2 = this.orm.insert(Member.class).value("name", "m2")
			.executeSelect();
		Member member3 = this.orm.insert(Member.class).value("name", "m3")
			.executeSelect();

		Member got = this.orm.singleBySQL(Member.class,
			"SELECT * FROM member WHERE name=?", Arrays.asList("m2"))
			.get();
		assertEquals(got.getId(), member2.getId());
		assertEquals(got.getName(), "m2");
	}

	@SuppressWarnings({"unused", "UnusedAssignment"})
	@Test
	public void singleWithStmt() throws SQLException, RichSQLException {
		Member member1 = this.orm.insert(Member.class).value("name", "m1")
			.executeSelect();
		Member member2 = this.orm.insert(Member.class).value("name", "m2")
			.executeSelect();
		Member member3 = this.orm.insert(Member.class).value("name", "m3")
			.executeSelect();

		{
			Member got = this.orm.single(Member.class).where("name=?", "m2").execute().get();
			assertEquals(got.getId(), member2.getId());
			assertEquals(got.getName(), "m2");
		}

		// With force write connection
		{
			Member got = this.orm.single(Member.class).forceWriteConnection().where("name=?", "m2").execute().get();
			assertEquals(got.getId(), member2.getId());
			assertEquals(got.getName(), "m2");
		}
	}

	@Test
	public void searchWithPager() throws SQLException, RichSQLException {
		IntStream.rangeClosed(1, 10).forEach(i -> {
			try {
				this.orm.insert(Member.class).value("name", "m" + i)
					.executeSelect();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		{
			Paginated<Member> paginated = this.orm.searchWithPager(
				Member.class, 4)
				.offset(0)
				.execute();
			assertEquals(4, paginated.getRows().size());
			assertEquals(4, paginated.getEntriesPerPage());
			assertEquals(true, paginated.getHasNextPage());
		}
		{
			Paginated<Member> paginated = this.orm.searchWithPager(
				Member.class, 4)
				.offset(4)
				.execute();
			assertEquals(4, paginated.getRows().size());
			assertEquals(4, paginated.getEntriesPerPage());
			assertEquals(true, paginated.getHasNextPage());
		}
		{
			Paginated<Member> paginated = this.orm.searchWithPager(
				Member.class, 4)
				.offset(8)
				.execute();
			assertEquals(2, paginated.getRows().size());
			assertEquals(4, paginated.getEntriesPerPage());
			assertEquals(false, paginated.getHasNextPage());
		}
		{
			Paginated<Member> paginated = this.orm.searchWithPager(
				Member.class, 4)
				.offset(12)
				.execute();
			assertEquals(0, paginated.getRows().size());
			assertEquals(4, paginated.getEntriesPerPage());
			assertEquals(false, paginated.getHasNextPage());
		}
		{
			Paginated<Member> paginated = this.orm.searchWithPager(
				Member.class, 5)
				.offset(0)
				.execute();
			assertEquals(5, paginated.getRows().size());
			assertEquals(5, paginated.getEntriesPerPage());
			assertEquals(true, paginated.getHasNextPage());
		}

		// Force write connection
		{
			Paginated<Member> paginated = this.orm.searchWithPager(Member.class, 4).forceWriteConnection().offset(
					0).execute();
			assertEquals(4, paginated.getRows().size());
			assertEquals(4, paginated.getEntriesPerPage());
			assertEquals(true, paginated.getHasNextPage());
		}
		{
			Paginated<Member> paginated = this.orm.searchWithPager(Member.class, 4).forceWriteConnection().offset(
					4).execute();
			assertEquals(4, paginated.getRows().size());
			assertEquals(4, paginated.getEntriesPerPage());
			assertEquals(true, paginated.getHasNextPage());
		}
		{
			Paginated<Member> paginated = this.orm.searchWithPager(Member.class, 4).forceWriteConnection().offset(
					8).execute();
			assertEquals(2, paginated.getRows().size());
			assertEquals(4, paginated.getEntriesPerPage());
			assertEquals(false, paginated.getHasNextPage());
		}
		{
			Paginated<Member> paginated = this.orm.searchWithPager(Member.class, 4).forceWriteConnection().offset(
					12).execute();
			assertEquals(0, paginated.getRows().size());
			assertEquals(4, paginated.getEntriesPerPage());
			assertEquals(false, paginated.getHasNextPage());
		}
		{
			Paginated<Member> paginated = this.orm.searchWithPager(Member.class, 5).forceWriteConnection().offset(
					0).execute();
			assertEquals(5, paginated.getRows().size());
			assertEquals(5, paginated.getEntriesPerPage());
			assertEquals(true, paginated.getHasNextPage());
		}
	}

	@Test
	public void testSearchBySQL() throws RichSQLException {
		for (int i = 1; i <= 10; ++i) {
			this.orm.insert(Member.class).value("name", "m" + i)
				.execute();
		}
		for (int i = 1; i <= 10; ++i) {
			this.orm.insert(Blog.class)
				.value("memberId", i)
				.value("title", "t" + i)
				.execute();
		}

		{
			List<Member> members = this.orm
				.searchBySQL(
					Member.class,
					"SELECT id, id+1 AS idPlusOne FROM member ORDER BY id DESC",
					Collections.emptyList());
			System.out.println(members);
			assertEquals(10, members.size());
			assertEquals("10,9,8,7,6,5,4,3,2,1", members.stream()
				.map(row -> "" + row.getId())
				.collect(Collectors.joining(",")));
			assertEquals("11,10,9,8,7,6,5,4,3,2", members.stream()
				.map(row -> "" + row.getExtraColumn("idPlusOne"))
				.collect(Collectors.joining(",")));
			assertEquals("11,10,9,8,7,6,5,4,3,2", members.stream()
				.map(row -> "" + row.getExtraColumns().get("idPlusOne"))
				.collect(Collectors.joining(",")));
		}
		// With specified connection
		{
			List<Member> members = this.orm.searchBySQL(Member.class,
					"SELECT id, id+1 AS idPlusOne FROM member ORDER BY id DESC", Collections.emptyList(),
					orm.getConnection());
			System.out.println(members);
			assertEquals(10, members.size());
			assertEquals("10,9,8,7,6,5,4,3,2,1",
					members.stream().map(row -> "" + row.getId()).collect(Collectors.joining(",")));
			assertEquals("11,10,9,8,7,6,5,4,3,2",
					members.stream().map(row -> "" + row.getExtraColumn("idPlusOne")).collect(Collectors.joining(",")));
			assertEquals("11,10,9,8,7,6,5,4,3,2",
					members.stream().map(row -> "" + row.getExtraColumns().get("idPlusOne")).collect(
							Collectors.joining(",")));
		}
		// respect "AS" label.
		{
			List<Blog> members = this.orm
				.searchBySQL(
					Blog.class,
					"SELECT blog.*, member.name AS memberName FROM member INNER JOIN blog ON (blog.memberId=member.id) ORDER BY id DESC LIMIT 1",
					Collections.emptyList());
			assertEquals(1, members.size());
			System.out.println(members.get(0).getExtraColumns());
			assertEquals("m10", members.get(0).getExtraColumn("memberName"));
		}
	}

	@Test
	public void testSearchBySQLWithPager() throws SQLException, RichSQLException {
		IntStream.rangeClosed(1, 10).forEach(i -> {
			try {
				this.orm.insert(Member.class).value("name", "m" + i)
					.executeSelect();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		{
			Paginated<Member> paginated = this.orm.searchBySQLWithPager(
				Member.class, "SELECT * FROM member ORDER BY id DESC",
				Collections.emptyList(), 4);
			assertEquals(paginated.getRows().size(), 4);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), true);
			assertEquals("10,9,8,7", paginated.getRows().stream()
				.map(row -> "" + row.getId())
				.collect(Collectors.joining(",")));
		}
		{
			Paginated<Member> paginated = this.orm.searchBySQLWithPager(
				Member.class,
				"SELECT * FROM member WHERE id<7 ORDER BY id DESC",
				Collections.emptyList(), 4);
			assertEquals(paginated.getRows().size(), 4);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), true);
			assertEquals("6,5,4,3", paginated.getRows().stream()
				.map(row -> "" + row.getId())
				.collect(Collectors.joining(",")));
		}
		{
			Paginated<Member> paginated = this.orm.searchBySQLWithPager(
				Member.class,
				"SELECT * FROM member WHERE id<? ORDER BY id DESC",
				Arrays.asList(3), 4);
			assertEquals(paginated.getRows().size(), 2);
			assertEquals(paginated.getEntriesPerPage(), 4);
			assertEquals(paginated.getHasNextPage(), false);
			assertEquals("2,1", paginated.getRows().stream()
				.map(row -> "" + row.getId())
				.collect(Collectors.joining(",")));
		}
		{
			Paginated<Member> paginated = this.orm.searchBySQLWithPager(
				Member.class,
				"SELECT * FROM member WHERE id<? ORDER BY id DESC",
				Arrays.asList(3), 10);
			assertEquals(paginated.getRows().size(), 2);
			assertEquals(paginated.getEntriesPerPage(), 10);
			assertEquals(paginated.getHasNextPage(), false);
			assertEquals("2,1", paginated.getRows().stream()
				.map(row -> "" + row.getId())
				.collect(Collectors.joining(",")));
		}
	}

	@SuppressWarnings({"unused", "UnusedAssignment"})
	@Test
	public void searchWithStmt() throws SQLException, RichSQLException {
		Member member1 = this.orm.insert(Member.class).value("name", "m1")
			.executeSelect();
		Member member2 = this.orm.insert(Member.class).value("name", "m2")
			.executeSelect();
		Member member3 = this.orm.insert(Member.class).value("name", "b1")
			.executeSelect();

		{
			List<Member> got = this.orm.search(Member.class).where("name LIKE ?", "m%").orderBy("id DESC").execute();
			assertEquals(got.size(), 2);
			assertEquals(got.get(0).getName(), "m2");
			assertEquals(got.get(1).getName(), "m1");
		}
		// Force write connection
		{
			List<Member> got = this.orm.search(Member.class).forceWriteConnection().where("name LIKE ?", "m%").orderBy(
					"id DESC").execute();
			assertEquals(got.size(), 2);
			assertEquals(got.get(0).getName(), "m2");
			assertEquals(got.get(1).getName(), "m1");
		}
	}

	@Test
	public void testMapRowsFromResultSet() throws SQLException {
		this.orm.getConnection()
			.prepareStatement(
				"INSERT INTO member (name, createdOn, updatedOn) VALUES ('m1', UNIX_TIMESTAMP(NOW()), UNIX_TIMESTAMP(NOW()))")
			.executeUpdate();
		try (PreparedStatement ps = this.orm.getConnection().prepareStatement(
			"SELECT * FROM member")) {
			try (ResultSet rs = ps.executeQuery()) {
				this.orm.mapRowListFromResultSet(Member.class, rs);
			}
		}
	}

	@Test
	public void testExecuteQuery() throws SQLException, RichSQLException {
		this.orm.executeQuery("SELECT 1");
		this.orm.executeQuery("SELECT 2");
	}

	@Test
	public void testExecuteQueryWithList() throws SQLException,
			RichSQLException {
		this.orm.executeQuery("SELECT 1+?", Arrays.asList(3));
	}

	@Test
	public void testExecuteQueryWithCallback() throws SQLException,
			RichSQLException {
		orm.insert(Member.class)
			.value("name", "John")
			.execute();
		orm.insert(Member.class)
			.value("name", "Taro")
			.execute();

		{
			String got = orm.executeQuery("SELECT id, name FROM member ORDER BY id ASC", (ResultSet rs) -> {
				StringBuilder builder = new StringBuilder();
				while (rs.next()) {
					long id = rs.getLong(1);
					String name = rs.getString(2);
					builder.append(id).append(":").append(name).append("\n");
				}
				return builder.toString();
			});
			assertEquals("1:John\n2:Taro\n", got);
		}
		// With specified connection
		{
			String got = orm.executeQuery("SELECT id, name FROM member ORDER BY id ASC", (ResultSet rs) -> {
				StringBuilder builder = new StringBuilder();
				while (rs.next()) {
					long id = rs.getLong(1);
					String name = rs.getString(2);
					builder.append(id).append(":").append(name).append("\n");
				}
				return builder.toString();
			}, orm.getConnection());
			assertEquals("1:John\n2:Taro\n", got);
		}
	}

	@Test
	public void testExecuteStreamEmpty() throws Exception {
		String sql = "SELECT id, name FROM member ORDER BY id ASC";
		try (Stream<ResultSet> stream = orm.executeStream(sql)) {
			String got = stream
				.map(rs -> {
					try {
						return rs.getString(2);
					} catch (SQLException e) {
						throw new UncheckedRichSQLException(e);
					}
				})
				.collect(Collectors.joining("\n"));
			assertEquals("", got);
		}
	}

	@Test
	public void testExecuteStream() throws Exception {
		orm.insert(Member.class)
		   .value("name", "John")
		   .execute();
		orm.insert(Member.class)
		   .value("name", "Taro")
		   .execute();

		String sql = "SELECT id, name FROM member ORDER BY id ASC";
		try (Stream<ResultSet> stream = orm.executeStream(sql)) {
			String got = stream
				.map(rs -> {
					try {
						long id = rs.getLong(1);
						String name = rs.getString(2);
						return String.valueOf(id) + ':' + name;
					} catch (SQLException e) {
						throw new UncheckedRichSQLException(e);
					}
				})
				.collect(Collectors.joining("\n"));
			assertEquals("1:John\n2:Taro", got);
		}
	}

	@Test
	public void testQueryForLong() throws SQLException, RichSQLException {
		assertEquals(1, this.orm.updateBySQL(
			"INSERT INTO foo (y,z) values (5963, 'hey')"
			));
		{
			OptionalLong got = this.orm
				.queryForLong("SELECT y FROM foo WHERE z='hey'");
			assertThat(got.isPresent(), is(true));
			assertThat(got.getAsLong(), is(5963L));
		}
		{
			OptionalLong got = this.orm
				.queryForLong("SELECT y FROM foo WHERE z='nothing'");
			assertThat(got.isPresent(), is(false));
		}
		// with placeholders
		{
			OptionalLong got = this.orm
				.queryForLong("SELECT y FROM foo WHERE z=?",
					Arrays.asList("hey"));
			assertThat(got.isPresent(), is(true));
			assertThat(got.getAsLong(), is(5963L));
		}
		{
			OptionalLong got = this.orm
				.queryForLong("SELECT y FROM foo WHERE z=?",
					Arrays.asList("Nothing"));
			assertThat(got.isPresent(), is(false));
		}
	}

	@Test
	public void testQueryForLongWithSpecifiedConnection() throws SQLException, RichSQLException {
		assertEquals(1, this.orm.updateBySQL("INSERT INTO foo (y,z) VALUES (5963, 'hey')"));
		{
			OptionalLong got = this.orm.queryForLong("SELECT y FROM foo WHERE z='hey'", orm.getConnection());
			assertThat(got.isPresent(), is(true));
			assertThat(got.getAsLong(), is(5963L));
		}
		{
			OptionalLong got = this.orm.queryForLong("SELECT y FROM foo WHERE z='nothing'", orm.getConnection());
			assertThat(got.isPresent(), is(false));
		}
		// with placeholders
		{
			OptionalLong got = this.orm.queryForLong("SELECT y FROM foo WHERE z=?", Arrays.asList("hey"),
					orm.getConnection());
			assertThat(got.isPresent(), is(true));
			assertThat(got.getAsLong(), is(5963L));
		}
		{
			OptionalLong got = this.orm.queryForLong("SELECT y FROM foo WHERE z=?", Arrays.asList("Nothing"),
					orm.getConnection());
			assertThat(got.isPresent(), is(false));
		}
	}

	@Test
	public void testQueryForString() throws SQLException, RichSQLException {
		this.orm.updateBySQL(
			"INSERT INTO bar (y,z) values ('ho', 'hey'), (NULL, 'yo')"
			);
		{
			Optional<String> got = this.orm
				.queryForString("SELECT y FROM bar WHERE z='hey'");
			assertThat(got.isPresent(), is(true));
			assertThat(got.get(), is("ho"));
		}
		{
			Optional<String> got = this.orm
					.queryForString("SELECT y FROM bar WHERE z='yo'");
			assertThat(got.isPresent(), is(false));
		}
		{
			Optional<String> got = this.orm
				.queryForString("SELECT y FROM bar WHERE z='nothing'");
			assertThat(got.isPresent(), is(false));
		}
		// with placeholders
		{
			Optional<String> got = this.orm
				.queryForString("SELECT y FROM bar WHERE z=?",
					Arrays.asList("hey"));
			assertThat(got.isPresent(), is(true));
			assertThat(got.get(), is("ho"));
		}
		{
			Optional<String> got = this.orm
				.queryForString("SELECT y FROM bar WHERE z=?",
					Arrays.asList("Nothing"));
			assertThat(got.isPresent(), is(false));
		}
	}

	@Test
	public void testQueryForStringWithSpecifiedConnection() throws SQLException, RichSQLException {
		this.orm.updateBySQL("INSERT INTO bar (y,z) VALUES ('ho', 'hey'), (NULL, 'yo')");
		{
			Optional<String> got = this.orm.queryForString("SELECT y FROM bar WHERE z='hey'", orm.getConnection());
			assertThat(got.isPresent(), is(true));
			assertThat(got.get(), is("ho"));
		}
		{
			Optional<String> got = this.orm.queryForString("SELECT y FROM bar WHERE z='yo'", orm.getConnection());
			assertThat(got.isPresent(), is(false));
		}
		{
			Optional<String> got = this.orm.queryForString("SELECT y FROM bar WHERE z='nothing'", orm.getConnection());
			assertThat(got.isPresent(), is(false));
		}
		// with placeholders
		{
			Optional<String> got = this.orm.queryForString("SELECT y FROM bar WHERE z=?", Arrays.asList("hey"),
					orm.getConnection());
			assertThat(got.isPresent(), is(true));
			assertThat(got.get(), is("ho"));
		}
		{
			Optional<String> got = this.orm.queryForString("SELECT y FROM bar WHERE z=?", Arrays.asList("Nothing"),
					orm.getConnection());
			assertThat(got.isPresent(), is(false));
		}
	}

	@Test
	public void testCount() throws SQLException, RichSQLException {
		this.orm.insert(Member.class).value("name", "m1")
			.execute();
		this.orm.insert(Member.class).value("name", "m2")
			.execute();
		this.orm.insert(Member.class).value("name", "b1")
			.execute();

		{
			long count = this.orm.count(Member.class)
				.execute();
			assertEquals(3, count);
		}
		{
			long count = this.orm.count(Member.class)
				.where("name LIKE 'm%'")
				.execute();
			assertEquals(2, count);
		}
		{
			long count = this.orm.count(Member.class)
				.where("name LIKE CONCAT(?, '%')", "b")
				.execute();
			assertEquals(1, count);
		}
	}

	@Test
	public void testCountForceWriteConnection() throws SQLException, RichSQLException {
		this.orm.insert(Member.class).value("name", "m1").execute();
		this.orm.insert(Member.class).value("name", "m2").execute();
		this.orm.insert(Member.class).value("name", "b1").execute();

		{
			long count = this.orm.count(Member.class).forceWriteConnection().execute();
			assertEquals(3, count);
		}
		{
			long count = this.orm.count(Member.class).forceWriteConnection().where("name LIKE 'm%'").execute();
			assertEquals(2, count);
		}
		{
			long count = this.orm.count(Member.class).forceWriteConnection().where("name LIKE CONCAT(?, '%')",
					"b").execute();
			assertEquals(1, count);
		}
	}

	@Test
	public void testClose() {
		try (Connection connection = buildConnection()) {
			assertNotNull(connection);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void testRenewClosedConnection() throws SQLException {
		{
			final Provider<Connection> connectionProvider = TestBase::buildConnection;
			final Provider<Connection> readConnectionProvider = TestBase::buildConnection;
			orm = new TinyORM(connectionProvider, readConnectionProvider);
			assertSame(this.orm.getConnection(), this.orm.getConnection());
			assertSame(this.orm.getReadConnection(), this.orm.getReadConnection());
		}
		// get new connection after close method called (in case of using connection pool)
		{
			final Provider<Connection> connectionProvider = TestBase::buildConnection;
			final Provider<Connection> readConnectionProvider = TestBase::buildConnection;
			orm = new TinyORM(connectionProvider, readConnectionProvider);
			final Connection oldConnection = this.orm.getConnection();
			final Connection oldReadConnection = this.orm.getReadConnection();
			assertFalse(oldConnection.isClosed());
			assertFalse(oldReadConnection.isClosed());

			orm.close();

			final Connection newConnection = this.orm.getConnection();
			final Connection newReadConnection = this.orm.getReadConnection();
			assertFalse(newConnection.isClosed());
			assertFalse(newReadConnection.isClosed());
			assertNotSame(oldConnection, newConnection);
			assertNotSame(oldReadConnection, newReadConnection);
		}
	}

	@Test
	public void testGetTableName() {
		assertEquals("member", this.orm.getTableName(Member.class));
	}

	@Test
	public void testGetColumnLabels() throws Exception {
		this.orm.getConnection()
				.prepareStatement(
						"INSERT INTO member (name, createdOn, updatedOn) VALUES ('m1', UNIX_TIMESTAMP(NOW()), UNIX_TIMESTAMP(NOW()))")
				.executeUpdate();
		try (PreparedStatement ps = this.orm.getConnection().prepareStatement(
				"SELECT * FROM member")) {
			try (ResultSet rs = ps.executeQuery()) {
				List<String> columnLabels = TinyORM.getColumnLabels(rs);
				assertEquals("id,name,createdOn,updatedOn", columnLabels.stream().collect(Collectors.joining(",")));
			}
		}
	}

	@Table("member")
	@Data
	@EqualsAndHashCode(callSuper = false)
	private static class Member extends Row<Member> {
		@PrimaryKey
		private long id;
		@Column
		private String name;

		@CreatedTimestampColumn
		private long createdOn;
		@UpdatedTimestampColumn
		private long updatedOn;
	}

	@Table("blog")
	@Data
	@EqualsAndHashCode(callSuper = false)
	private static class Blog extends Row<Blog> {
		@PrimaryKey
		private long id;
		@Column
		private String title;

		@CreatedTimestampColumn
		private long createdOn;
		@UpdatedTimestampColumn
		private long updatedOn;
	}

	@Data
	private static class MemberForm {

		private long id;
		private String name;
	}

	@Data
	public static class MemberUpdateForm {
		private final String name;

		public MemberUpdateForm(String name) {
			this.name = name;
		}
	}

	@Test(expected = com.mysql.jdbc.exceptions.MySQLTimeoutException.class)
	public void testQueryForLongTimeout() throws Throwable {
		orm.setQueryTimeout(1);
		try {
			final OptionalLong optionalLong = orm.queryForLong("SELECT SLEEP(3)");
		} catch (UncheckedRichSQLException e) {
			throw e.getCause().getCause();
		}
	}

	@Test
	public void testQueryForLongTimeoutPassed() throws Throwable {
		orm.setQueryTimeout(1);
		final OptionalLong optionalLong = orm.queryForLong("SELECT 3");
		assertEquals(3, optionalLong.getAsLong());
	}

}
