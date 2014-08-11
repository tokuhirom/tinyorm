package me.geso.tinyorm;

public class MemberUpdateForm {
	private String name;
	
	public MemberUpdateForm(String name) {
		this.name = name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return this.name;
	}
}
