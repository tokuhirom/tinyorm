package me.geso.tinyorm;

// I know commons-lang. But I don't want to depend it only for this method...
class ObjectUtils {

	static boolean _equals(Object a, Object b) {
		if (a != null) {
			return a.equals(b);
		} else {
			return b == null;
		}
	}

}
