package me.geso.tinyorm;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class PaginatedTest {

	@Test
	public void testMapToBean() {
		int entriesPerPage = 10;
		boolean hasNextPage = true;
		List<Source> sourceList = new ArrayList<>();
		sourceList.add(new Source("hoge"));
		Paginated<Source> p1 = new Paginated<>(sourceList, entriesPerPage, hasNextPage);
		Paginated<Dest> mapToBean = p1.mapToBean(Dest.class);
		assertEquals(mapToBean.getRows().size(), 1);
		assertEquals(mapToBean.getRows().get(0).getN(), "hoge");
		assertEquals(mapToBean.getEntriesPerPage(), 10);
		assertEquals(mapToBean.getHasNextPage(), true);
	}
	
	public static class Source {
		private String n;
		
		public Source(String n) {
			this.n = n;
		}

		public void setN(String n) {
			this.n = n;
		}
		public String getN() {
			return this.n;
		}
	}

	public static class Dest {
		private String n;

		public void setN(String n) {
			this.n = n;
		}
		public String getN() {
			return this.n;
		}
	}

}
