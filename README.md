tinyorm
=======

[![Build Status](https://travis-ci.org/tokuhirom/tinyorm.svg?branch=master)](https://travis-ci.org/tokuhirom/tinyorm)

This is a tiny o/r mapper for Java 8.

## Setup

main/java/myapp/DB.java

    public DB extends TinyORM {
      public void getConnection() {
        return Context.getContext().getConnection();
      }
    }

main/java/myapp/rows/Member.java

    @Table("member")
    @Data // lombok
    @EqualsAndHashCode(callSuper = false)
    public Member extends ActiveRecord<Member> {
      private long id;
      private String name;
    }

## Examples

Create new database object.

    DB db = new DB();

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

## HOOKS

You can override `TinyORM#BEFORE_INSERT` and `TinyORM#BEFORE_UPDATE` methods.
You can fill createdOn and updatedOn columns by this.

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
