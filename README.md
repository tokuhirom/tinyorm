tinyorm
=======

[![Build Status](https://travis-ci.org/tokuhirom/tinyorm.svg?branch=master)](https://travis-ci.org/tokuhirom/tinyorm)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/me.geso/tinyorm/badge.svg)](https://maven-badges.herokuapp.com/maven-central/me.geso/tinyorm)

This is a tiny o/r mapper for Java 8.

## Setup

main/java/myapp/rows/Member.java

```java
@Table("member")
@Data // lombok
@EqualsAndHashCode(callSuper = false)
public Member extends Row<Member> {
  private long id;
  private String name;
}
```

## Examples

Create new database object.

```java
Connection connection = DriverManager.getConnection(dburl, dbuser, dbpassword);
TinyORM db = new TinyORM(connection);
```

Or you can instantiate with lazily connection borrowing, like so;

```java
Provider<Connection> connectionProvider = ConnectionPool.borrow();
TinyORM db = new TinyORM(connectionPooling);
```

Please see [Connection](#connection) section.

### Selecting one row.

```java
Optional<Member> member = db.single(Member.class)
  .where("id=?", 1)
  .execute();
```

### Selecting rows.

```java
List<Member> member = db.search(Member.class)
  .where("name LIKE CONCAT(?, '%')", "John")
  .execute();
```

### Insert row

```java
db.insert(Member.class)
  .value("name", "John")
  .execute();
```

This statement generate following query:

```sql
INSERT INTO `member` (name) VALUES ('John')
```

If you want to use `ON DUPLICATE KEY UPDATE`, you can call `InsertStatement#onDuplicateKeyUpdate` method.

For example:

```java
orm.insert(Member.class)
  .value("email", email)
  .value("name", name)
  .onDuplicateKeyUpdate("name=?", name)
  .execute();
```

### Insert row with form class.

```java
@Data // lombok
class MemberInsertForm {
  private String name;
}

MemberInsertForm form = new MemberInsertForm();
form.name = name;
db.insert(Member.class).valueByBean(form).execute();
```

### Update row with form class.

```java
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
```

### Delete row

```java
Member member = db.single(Member.class)
  .where("id=?", 1)
  .execute()
  .get();
member.delete();
```

## Annotations

```java
@Value
@Table("member")
@EqualsAndHashCode(callSuper = false)
public MemberRow extends Row<MemberRow> {
    @PrimaryKey
    private long id;
    @Column
    private String name;
    @Column("alias_name")
    private String aliasName;
    @CreatedTimestampColumn
    private long createdOn;
}
```

### @PrimaryKey

You need to add this annotation for the field, that is a primary key.

### @Column

You need to add this annotation for each columns(Note, if you specified @PrimaryKey, @CretedOnTimeStamp or @UpdatedOnTimeStamp, you don't need to specify this annotaiton).

### @Column("column_name")

This annotation is almost the same as `@Column`. A point of difference; this annotation can specify a column name without regard for member variable name.

### @CreatedOnTimeStamp

TinyORM fills this field when inserting a row. This column must be `long`. TinyORM fills epoch seconds.

### @UpdatedOnTimeStamp

TinyORM fills this field when updating a row. This column must be `long`. TinyORM fills epoch seconds.

### @CsvColumn

```java
@CsvColumn
private List<String> prefectures;
```

You can store the data in CSV format.

### @JsonColumn

```java
@JsonColumn
private MyComplexType myComplexThing;
```

You can store the data in JSON format.

### @SetColumn

```java
@SetColumn
private Set<String> categories;
```

TinyORM conerts MySQL's SET value as java.util.Set.

## HOOKS

You can override `TinyORM#BEFORE_INSERT` and `TinyORM#BEFORE_UPDATE` methods.
You can fill createdOn and updatedOn columns by this.

## How do I use java.time.LocalDate?

You can use java.time.LocalDate for the column field.

```java
@Value
@EqualsAndHashCode(callSuper = false)
@Table("foo")
public class Foo extends Row<Foo> {
    @Column
    private LocalDate date;
}
```

TinyORM automatically convert java.sql.Date to java.time.LocalDate.

## HOW DO I WRITE MY OWN CONSTRUCTOR?

You can create your own constructor, and create it from the constructor, you need to add `java.beans.ConstructorProperties` annotation.

```java
public class RowExample {
    @java.beans.ConstructorProperties({"name", "age", "score", "tags"})
    public RowExample(String name, long age, long score, String tags) {
       // ...
    }
}
```

Normally, you should use lombok.Value to create constructor.

## Connection

### Supports handling two divided connections

TinyORM can use two connections, `read/write` connection and `read only` connection.

TinyORM decide automatically which connection to use depending on the situation.
It means, TinyORM uses `read only` connection when handling reading query.
On the other hand, that uses `read/write` connection when handling writing query or handling some queries in a transaction.

It ensures using the same `read/write` connection in a transaction.

#### How to use two connections

```java
Connection connection = DriverManager.getConnection(dburl, dbuser, dbpassword);
Connection readConnection = DriverManager.getConnection(readDburl, readDbuser, readDbpassword);
TinyORM db = new TinyORM(connection, readconnection);
```

If you pass only one connection to the constructor, TinyORM treats that connection as `read/write` connection and always uses that.

### Supports lazily connection borrowing

If you pass the type of `Provider<Connection>` value to the constructor,
TinyORM defers borrowing (or establishing) connection until the connection is needed.

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
