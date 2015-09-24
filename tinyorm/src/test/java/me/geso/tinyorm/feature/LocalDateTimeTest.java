package me.geso.tinyorm.feature;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.tinyorm.Row;
import me.geso.tinyorm.TestBase;
import me.geso.tinyorm.TinyORM;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

/**
 * Use LocalDate for "DATE" column.
 */
public class LocalDateTimeTest extends TestBase {
    @Test
    public void test() throws Exception {
        this.createTable(
                "x",
                "id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
                "dt DATETIME NOT NULL"
        );

        final LocalDateTime dt = LocalDateTime.parse("2015-01-01T03:04");
        final TinyORM tinyORM = new TinyORM(connection);
        final X x = tinyORM.insert(X.class)
                           .value("dt", dt)
                           .executeSelect();
        assertEquals("2015-01-01T03:04:00", x.getDt().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        assertEquals("2015-01-01T03:04:00", x.refetch().get().getDt().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }

    @Test
    public void testNull() throws Exception {
        this.createTable(
                "x",
                "id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
                "dt DATETIME DEFAULT NULL"
        );

        final TinyORM tinyORM = new TinyORM(connection);
        final X x = tinyORM.insert(X.class)
                           .value("dt", null)
                           .executeSelect();
        assertNull(x.getDt());
        assertNull(x.refetch().get().getDt());
    }

    @Table("x")
    @Value
    @EqualsAndHashCode(callSuper = false)
    public static class X extends Row<X> {
        @PrimaryKey
        private long id;
        @Column
        private LocalDateTime dt;
    }
}

