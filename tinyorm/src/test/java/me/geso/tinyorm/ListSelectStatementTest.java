package me.geso.tinyorm;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

public class ListSelectStatementTest extends TestBase {
    @Before
    public void before() {
        createTable("member",
                    "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY",
                    "name VARCHAR(255) NOT NULL");
    }

    @Test
    public void testExecuteWithPagination() {
        // empty result
        {
            Paginated<Member> list = orm.search(Member.class).orderBy("id DESC").executeWithPagination(1);
            assertThat(list.getRows().size(), is(0));
            assertThat(list.getEntriesPerPage(), is(1L));
            assertThat(list.getHasNextPage(), is(false));
        }
        for (int i = 0; i<3; i++) {
            orm.insert(Member.class).value("name", "name").execute();
        }
        // first page
        {
            Paginated<Member> list = orm.search(Member.class).orderBy("id DESC").executeWithPagination(1);
            assertThat(list.getRows().size(), is(1));
            assertThat(list.getEntriesPerPage(), is(1L));
            assertThat(list.getHasNextPage(), is(true));
        }
        // last page
        {
            Paginated<Member> list = orm.search(Member.class).orderBy("id DESC").executeWithPagination(3);
            assertThat(list.getRows().size(), is(3));
            assertThat(list.getEntriesPerPage(), is(3L));
            assertThat(list.getHasNextPage(), is(false));
        }
        // last page (less than entries per page)
        {
            Paginated<Member> list = orm.search(Member.class).orderBy("id DESC").executeWithPagination(4);
            assertThat(list.getRows().size(), is(3));
            assertThat(list.getEntriesPerPage(), is(4L));
            assertThat(list.getHasNextPage(), is(false));
        }
    }

    @Test
    public void testExecuteStreamEmpty() throws Exception {
        try (Stream<Member> stream = orm.search(Member.class)
                                        .orderBy("id DESC")
                                        .executeStream()) {
            assertThat(
                    stream.map(Member::getName).collect(Collectors.joining(",")),
                    is(""));
        }
    }

    @Test
    public void testExecuteStream() throws Exception {
        orm.insert(Member.class)
           .value("name", "John")
           .execute();
        orm.insert(Member.class)
           .value("name", "Nick")
           .execute();
        try (Stream<Member> stream = orm.search(Member.class)
                                        .orderBy("id DESC")
                                        .executeStream()) {
            assertThat(
                    stream.map(Member::getName).collect(Collectors.joining(",")),
                    is("Nick,John"));
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @Value
    @Table("member")
    public static class Member extends Row<Member> {
        @PrimaryKey
        private long id;
        @Column
        private String name;
    }
}
