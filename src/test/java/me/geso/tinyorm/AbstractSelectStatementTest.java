package me.geso.tinyorm;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import lombok.Data;
import lombok.EqualsAndHashCode;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;

public class AbstractSelectStatementTest extends TestBase {
	@Before
	public final void setupSchema() throws RichSQLException {
		this.orm.updateBySQL("DROP TABLE IF EXISTS member");
		this.orm.updateBySQL(
			"CREATE TABLE member ("
				+ "id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
				+ "name VARCHAR(255),"
				+ "createdOn INT UNSIGNED DEFAULT NULL,"
				+ "updatedOn INT UNSIGNED DEFAULT NULL"
				+ ")"
			);
	}

	@Test
	public void singleForUpdate() throws SQLException, RichSQLException {
		assertThat(this.orm.single(Member.class).buildQuery().getSQL(),
			is("SELECT * FROM `member`"));
		assertThat(this.orm.single(Member.class).forUpdate().buildQuery()
			.getSQL(),
			is("SELECT * FROM `member` FOR UPDATE"));
	}

	@Table("member")
	@Data
	@EqualsAndHashCode(callSuper = false)
	public static class Member extends Row<Member> {
		@PrimaryKey
		private long id;
		@Column
		private String name;

		@CreatedTimestampColumn
		private long createdOn;
		@UpdatedTimestampColumn
		private long updatedOn;
	}
}
