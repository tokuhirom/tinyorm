package me.geso.tinyorm;

import lombok.ToString;
import me.geso.tinyorm.annotations.Column;
import me.geso.tinyorm.annotations.CreatedTimestampColumn;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;
import me.geso.tinyorm.annotations.UpdatedTimestampColumn;

/**
 * In your production code, I suggest to use lombok.
 *
 * @author Tokuhiro Matsuno <tokuhirom@gmail.com>
 */
@Table("member")
@ToString
public class Member extends BasicRow<Member> {

	@PrimaryKey
	private long id;
	@Column
	private String name;

	@Column @CreatedTimestampColumn
	private long createdOn;
	@Column @UpdatedTimestampColumn
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
