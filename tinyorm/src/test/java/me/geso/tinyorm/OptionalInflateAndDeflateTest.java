package me.geso.tinyorm;

import lombok.Getter;
import lombok.Setter;
import me.geso.tinyorm.annotations.*;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class OptionalInflateAndDeflateTest extends TestBase {
	@Before
	public void before() throws SQLException {
		orm.updateBySQL("DROP TABLE IF EXISTS x");
		createTable("x",
			"id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT",
			"name VARCHAR(255)");
		createTable("y",
			"id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
			"dt DATE");
	}

	@Test
	public void testForExistedValue() throws SQLException {
		X x = orm.insert(X.class)
			.value("name", Optional.of("John"))
			.executeSelect();
		Optional<String> maybeName = x.getName();
		assertEquals(true, maybeName.isPresent());
		assertEquals("John", maybeName.get());
	}

	@Test
	public void testForEmptyValue() {
		X x = orm.insert(X.class)
			.value("name", Optional.empty())
			.executeSelect();
		Optional<String> maybeName = x.getName();
		assertEquals(false, maybeName.isPresent());
	}

	@Test
	public void testForNullValue() {
		X x = orm.insert(X.class)
			.value("name", null)
			.executeSelect();
		Optional<String> maybeName = x.getName();
		assertEquals(false, maybeName.isPresent());
	}

	@Test
	public void testForMultistageInflationAndDeflation() {
		Optional<LocalDate> dt = Optional.of(LocalDate.parse("2015-01-01"));
		Y y = orm.insert(Y.class)
			.value("dt", dt)
			.executeSelect();
		assertEquals("2015-01-01", y.getDt().get().format(DateTimeFormatter.ISO_LOCAL_DATE));
		assertEquals("2015-01-01", y.refetch().get().getDt().get().format(DateTimeFormatter.ISO_LOCAL_DATE));
	}

	@Test
	public void testForMultistageInflationAndDeflationForEmptyValue() {
		Y y = orm.insert(Y.class)
			.value("dt", Optional.empty())
			.executeSelect();
		assertEquals(false, y.getDt().isPresent());
	}

	@Test
	public void testForMultistageInflationAndDeflationForNullValue() {
		Y y = orm.insert(Y.class)
			.value("dt", null)
			.executeSelect();
		assertEquals(false, y.getDt().isPresent());
	}

	@Getter
	@Setter
	@Table("x")
	public static class X extends Row<X> {
		@PrimaryKey
		long id;
		@Column
		Optional<String> name;
	}

	@Getter
	@Setter
	@Table("y")
	public static class Y extends Row<Y> {
		@PrimaryKey
		long id;
		@Column
		Optional<LocalDate> dt;
	}
}
