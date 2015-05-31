package me.geso.tinyorm;

import lombok.Getter;
import lombok.Setter;
import me.geso.jdbcutils.RichSQLException;
import me.geso.tinyorm.annotations.*;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class OptionalInflateAndDeflateTest extends TestBase {
    @Before
    public void before() throws SQLException {
        orm.updateBySQL("DROP TABLE IF EXISTS y");
        createTable("x",
                "id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT",
                "name VARCHAR(255)");
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

    @Getter
    @Setter
    @Table("x")
    public static class X extends Row<X> {
        @PrimaryKey
        long id;
        @Column
        Optional<String> name;
    }
}
