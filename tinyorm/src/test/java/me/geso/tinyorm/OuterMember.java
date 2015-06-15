package me.geso.tinyorm;

import lombok.EqualsAndHashCode;
import lombok.Value;
import me.geso.tinyorm.annotations.PrimaryKey;
import me.geso.tinyorm.annotations.Table;

/**
 * This is a class for testing "member class" detection.
 */
@Table("member")
@Value
@EqualsAndHashCode(callSuper = false)
private class OuterMember extends Row<OuterMember> {
	@PrimaryKey
	private long id;
}
