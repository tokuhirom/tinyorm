package me.geso.tinyorm;

import static org.junit.Assert.*;

import org.junit.Test;

public class ObjectUtilsTest {

	@Test
	public void test() {
		assertTrue(ObjectUtils._equals(null, null));
		assertFalse(ObjectUtils._equals(null, ""));
		assertFalse(ObjectUtils._equals("", null));
		assertTrue(ObjectUtils._equals("a", "a"));
		assertFalse(ObjectUtils._equals("a", "b"));
	}

}
