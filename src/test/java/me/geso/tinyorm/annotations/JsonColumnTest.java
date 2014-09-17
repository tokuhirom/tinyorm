package me.geso.tinyorm.annotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;

import org.junit.Test;

public class JsonColumnTest extends TestBase {

	@Test
	public void test() throws SQLException {
		orm.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS x")
				.executeUpdate();
		orm.getConnection()
				.prepareStatement(
						"CREATE TABLE x (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, propertiesDump LONGBLOB NOT NULL)")
				.executeUpdate();
		Map<String, String> map = new HashMap<>();
		map.put("hoge", "fuga");
		X created = orm.insert(X.class)
				.value("propertiesDump", map)
				.executeSelect();
		created = created.refetch().get();
		assertEquals("fuga", created.getPropertiesDump().get("hoge"));

		ResultSet rs = orm.getConnection()
				.prepareStatement(
						"SELECT propertiesDump FROM x")
				.executeQuery();
		assertTrue(rs.next());
		assertEquals("{\"hoge\":\"fuga\"}", new String(rs.getBytes(1), Charset.forName("UTF-8")));
		assertFalse(rs.next());
	}

	@Getter
	@Setter
	@Table("x")
	public static class X extends Row<X>{
		@PrimaryKey
		private long id;

		@JsonColumn
		private Map<String, String> propertiesDump;
	}

}
