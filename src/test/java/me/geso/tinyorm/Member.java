package me.geso.tinyorm;

/**
 * In your production code, I suggest to use lombok.
 *
 * @author Tokuhiro Matsuno <tokuhirom@gmail.com>
 */
@Table("member")
public class Member extends BasicRow<Member> {

	@PrimaryKey
	private long id;
	private String name;

	public void setId(long id) {
		this.id = id;
	}

	public long getId() {
		return this.id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
