tinyorm
=======

[![Build Status](https://travis-ci.org/tokuhirom/tinyorm.svg?branch=master)](https://travis-ci.org/tokuhirom/tinyorm)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/me.geso/tinyorm/badge.svg)](https://maven-badges.herokuapp.com/maven-central/me.geso/tinyorm)

This is a tiny o/r mapper for Java 8.

## Setup

main/java/myapp/rows/Member.java

    @Table("member")
    @Data // lombok
    @EqualsAndHashCode(callSuper = false)
    public Member extends Row<Member> {
      private long id;
      private String name;
    }

## Examples

Create new database object.

    TinyORM db = new TinyORM(connection);

### Selecting one row.

    Optional<Member> member = db.single(Member.class)
      .where("id=?", 1)
      .execute();

### Selecting rows.

    List<Member> member = db.single(Member.class)
      .where("name LIKE CONCAT(?, '%')", "John")
      .execute();

### Insert row

    db.insert(Member.class)
      .value("name", "John")
      .execute();

### Insert row with form class.

    @Data // lombok
    class MemberInsertForm {
      private String name;
    }
    
    MemberInsertForm form = new MemberInsertForm();
    form.name = name;
    db.insert(Member.class).valueByBean(form).execute();

### Update row with form class.

    @Data // lombok
    class MemberUpdateForm {
      private String name;
    }
    
    MemberUpdateForm form = new MemberUpdateForm();
    form.name = name;
    Member member = db.single(Member.class)
      .where("id=?", 1)
      .execute()
      .get();
    member.updateByBean(form);

### Delete row

    Member member = db.single(Member.class)
      .where("id=?", 1)
      .execute()
      .get();
    member.delete();

## Annotations

    @Value
    @Table("member")
    @EqualsAndHashCode(callSuper = false)
    public MemberRow extends Row<MemberRow> {
        @PrimaryKey
        private long id;
        @Column
        private String name;
        @ColumnName("alias_name")
        private String aliasName;
        @CreatedTimestampColumn
        private long createdOn;
    }

### @PrimaryKey

You need to add this annotation for the field, that is a primary key.

### @Column

You need to add this annotation for each columns(Note, if you specified @PrimaryKey, @ColumnName, @CretedOnTimeStamp or @UpdatedOnTimeStamp, you don't need to specify this annotaiton).

### @ColumnName("column_name")

This annotation is similar to `@Column`. A point of difference; this annotation can specify a column name without regard for member variable name.

### @CreatedOnTimeStamp

TinyORM fills this field when inserting a row. This column must be `long`. TinyORM fills epoch seconds.

### @UpdatedOnTimeStamp

TinyORM fills this field when updating a row. This column must be `long`. TinyORM fills epoch seconds.

### @CsvColumn

    @CsvColumn
    private List<String> prefectures;

You can store the data in CSV format.

## HOOKS

You can override `TinyORM#BEFORE_INSERT` and `TinyORM#BEFORE_UPDATE` methods.
You can fill createdOn and updatedOn columns by this.

## How do I use java.time.LocalDate?

You can use java.time.LocalDate for the column field.

    @Value
    @EqualsAndHashCode(callSuper = false)
    @Table("foo")
    public class Foo extends Row<Foo> {
        @Column
        private LocalDate date;
    }

TinyORM automatically convert java.sql.Date to java.time.LocalDate.

## LICENSE

  The MIT License (MIT)
  Copyright © 2014 Tokuhiro Matsuno, http://64p.org/ <tokuhirom@gmail.com>

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the “Software”), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
