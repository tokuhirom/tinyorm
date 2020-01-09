package me.geso.tinyorm;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.jdbcutils.Query;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

public class SelectCountStatementTest extends TestBase {

	@Before
	public void beforeHook() throws SQLException {
		orm.updateBySQL("DROP TABLE IF EXISTS `member`");
		orm.updateBySQL("CREATE TABLE `member` (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)");
	}

	@Test
	public void testWhere() throws Exception {
		Method buildQuery = SelectCountStatement.class.getDeclaredMethod("buildQuery");
		buildQuery.setAccessible(true);
		SelectCountStatement<Member> stmt = orm.count(Member.class)
			.where("id=?", 1);
		Query query = (Query)buildQuery.invoke(stmt);
		assertEquals("SELECT COUNT(*) FROM `member` WHERE (id=?)", query.getSQL());
	}

	@EqualsAndHashCode(callSuper = false)
	@Value
	@Table("member")
	private static class Member extends Row<Member> {
		@PrimaryKey
		private long id;
		@Column
		private String name;
	}
}
