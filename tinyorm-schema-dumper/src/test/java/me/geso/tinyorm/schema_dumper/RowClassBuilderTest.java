package me.geso.tinyorm.schema_dumper;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;

import lombok.Value;
import me.geso.dbinspector.Column;
import me.geso.dbinspector.Inspector;
import me.geso.dbinspector.Table;
import me.geso.tinyorm.schema_dumper.exception.UnknownTableException;
import me.geso.tinyorm.schema_dumper.exception.UnsupportedColumnTypeException;

public class RowClassBuilderTest extends TestBase {
	@Test
	public void testJavaFile() throws SQLException, UnsupportedColumnTypeException, UnknownTableException {
		createTable("member",
				"id int unsigned not null auto_increment primary key",
				"name varchar(255) binary not null");

		final RowClassBuilder rowClassBuilder = new RowClassBuilder(connection, "com.example");
		final JavaFile typeSpec =
				rowClassBuilder.buildBaseClassJavaFile("member");
		System.out.println(typeSpec.toString());
		assertThat(typeSpec.toString())
				.contains("public class Member extends Row<Member> ");
	}

	@Test
	public void testBuildBaseClass() throws SQLException, UnsupportedColumnTypeException {
		createTable("member",
			"id int unsigned not null auto_increment primary key",
			"name varchar(255) binary not null");

		final RowClassBuilder rowClassBuilder = new RowClassBuilder(connection, "com.example");
		final Inspector inspector = new Inspector(connection);
		final Table memberTable = inspector.getTable("member").get();
		final TypeSpec typeSpec =
				rowClassBuilder.buildBaseClass(memberTable);
		System.out.println(typeSpec.toString());
		assertThat(typeSpec.toString())
				.contains("public class Member extends me.geso.tinyorm.Row<com.example.Member> ");
	}

	@Test
	public void testGetJavaColumnType() throws SQLException, UnsupportedColumnTypeException {
		List<JavaColumnTypeTestCase> testCases = Arrays.asList(
			new JavaColumnTypeTestCase("VARCHAR(255)", "java.lang.String"),
			new JavaColumnTypeTestCase("INT UNSIGNED NOT NULL", "java.lang.Long"),
			new JavaColumnTypeTestCase("CHAR(255)", "java.lang.String"),
			new JavaColumnTypeTestCase("TEXT", "java.lang.String"),
			new JavaColumnTypeTestCase("TINYINT", "java.lang.Integer"),
			new JavaColumnTypeTestCase("BIGINT", "java.lang.Long"),
			new JavaColumnTypeTestCase("BIGINT UNSIGNED", "java.math.BigInteger"),
			new JavaColumnTypeTestCase("DECIMAL(10,3)", "java.math.BigDecimal"),
			new JavaColumnTypeTestCase("BLOB", "byte[]"),
			new JavaColumnTypeTestCase("LONGBLOB", "byte[]"),
			new JavaColumnTypeTestCase("INTEGER", "java.lang.Integer"),
			new JavaColumnTypeTestCase("ENUM('x', 'y')", "java.lang.String"),
			new JavaColumnTypeTestCase("TINYINT(1)", "java.lang.Boolean"),
			new JavaColumnTypeTestCase("BIT(3)", "byte[]"),
			new JavaColumnTypeTestCase("BOOLEAN", "java.lang.Boolean")
			);
		final RowClassBuilder rowClassBuilder = new RowClassBuilder(connection, "com.example");
		final Inspector inspector = new Inspector(connection);
		String tableName = "member";
		for (final JavaColumnTypeTestCase testCase : testCases) {
			createTable(tableName,
				"x " + testCase.getSqlType());
			final Table table = inspector.getTable(tableName).get();
			final Column column = table.getColumn("x");
			final FieldSpec fieldSpec = rowClassBuilder.buildColumnField(column);
			System.out.println(fieldSpec.toString());
			assertThat(fieldSpec.type.toString())
				.isEqualTo(testCase.getJavaType());
		}
	}

	@Value
	public static class JavaColumnTypeTestCase {
		private String sqlType;
		private String javaType;
	}

}
