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
	private long createdOn;
	private long updatedOn;

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

	// hook.
	@Override
	public void BEFORE_UPDATE(UpdateRowStatement stmt) {
		this.updatedOn = System.currentTimeMillis() / 1000;
		stmt.set("updatedOn", this.updatedOn);
	}

	public long getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(long createdOn) {
		this.createdOn = createdOn;
	}

	public long getUpdatedOn() {
		return updatedOn;
	}

	public void setUpdatedOn(long updatedOn) {
		this.updatedOn = updatedOn;
	}
}
