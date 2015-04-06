package me.geso.tinyorm.schema_dumper;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;

import lombok.Getter;
import lombok.Setter;
import me.geso.dbinspector.Inspector;
import me.geso.dbinspector.Table;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;
import me.geso.tinyorm.schema_dumper.exception.UnknownTableException;
import me.geso.tinyorm.schema_dumper.exception.UnsupportedColumnTypeException;

// TODO: support @CsvColumn
public class RowClassBuilder {
	private final Inspector inspector;
	@Getter
	@Setter
	private boolean tinyInt1isBit = true;
	private String packageName;

	public RowClassBuilder(Connection connection, String packageName) {
		this.packageName = packageName;
		this.inspector = new Inspector(connection);
	}

	private String ucfirst(String string) {
		return string.substring(0, 1).toUpperCase() + string.substring(1);
	}

	public JavaFile buildBaseClassJavaFile(final String tableName)
			throws UnsupportedColumnTypeException, SQLException, UnknownTableException {
		final Table table = inspector.getTable(tableName).orElseThrow(() -> new UnknownTableException(tableName));
		final TypeSpec typeSpec = buildBaseClass(table);
		return JavaFile.builder(packageName, typeSpec)
			.build();
	}

	public TypeSpec buildBaseClass(final Table table) throws UnsupportedColumnTypeException {
		String className = ucfirst(table.getName());

		final ParameterizedTypeName superclass = ParameterizedTypeName.get(ClassName.get(Row.class), ClassName.get(packageName, className));

		TypeSpec.Builder classBuilder = TypeSpec.classBuilder(className)
			.superclass(superclass)
			.addModifiers(Modifier.PUBLIC);

		for (final me.geso.dbinspector.Column column : table.getColumns()) {
			buildColumn(classBuilder, column);
		}

		return classBuilder.build();
	}

	public void buildColumn(TypeSpec.Builder classBuilder, final me.geso.dbinspector.Column column)
			throws UnsupportedColumnTypeException {
		classBuilder.addField(buildColumnField(column));
	}

	// TODO: createdOn
	// TODO: updatedOn
	public FieldSpec buildColumnField(final me.geso.dbinspector.Column column) throws UnsupportedColumnTypeException {
		return FieldSpec.builder(getJavaColumnType(column), column.getName(), Modifier.PUBLIC)
			.addAnnotation(getColumnAnnotation(column))
			.addJavadoc("name:$S dataType:$S typeName:$S size:$S\n", column.getName(), column.getDataType(), column.getTypeName(), column.getSize())
			.build();
	}

	private Class<?> getColumnAnnotation(final me.geso.dbinspector.Column column) {
		switch (column.getName()) {
			case "createdOn":
				return CreatedTimestampColumn.class;
			case "updatedOn":
				return UpdatedTimestampColumn.class;
			default:
				return me.geso.tinyorm.annotations.Column.class;
		}
	}

	/**
	 * See http://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
	 *
	 * <ul>
	 * <li>SET</li>
	 * <li>FLOAT</li>
	 * <li>REAL</li>
	 * <li>DOUBLE PRECISION</li>
	 * <li>NUMERIC</li>
	 * <li>SMALLINT</li>
	 * <li>MEDIUMINT</li>
	 * </ul>
	 */
	public Type getJavaColumnType(me.geso.dbinspector.Column column) throws UnsupportedColumnTypeException {
		switch (column.getTypeName()) {
			case "VARCHAR":
				return String.class;
			case "CHAR":
				return String.class;
			case "BLOB":
				return byte[].class;
			case "LONGBLOB":
				return byte[].class;
			case "INT":
				return Integer.class;
			case "INT UNSIGNED":
				return Long.class;
			case "TINYINT":
				if (column.getSize() == 1 && tinyInt1isBit) {
					return Boolean.class;
				} else {
					return Integer.class;
				}
			case "BIGINT":
				return Long.class;
			case "BIGINT UNSIGNED":
				return BigInteger.class;
			case "DECIMAL":
				return BigDecimal.class;
			case "ENUM":
				return String.class;
			case "BIT":
				if (column.getSize() == 0) {
					return Boolean.class;
				} else {
					return byte[].class;
				}
			case "TEXT":
				return String.class;
			default:
				throw new UnsupportedColumnTypeException(column);
		}
	}
}
