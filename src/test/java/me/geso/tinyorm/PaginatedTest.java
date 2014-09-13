package me.geso.tinyorm;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PaginatedTest {

	@Test
	public void testJackson() throws IOException {
		int entriesPerPage = 10;
		boolean hasNextPage = true;
		List<Source> sourceList = new ArrayList<>();
		sourceList.add(new Source("hoge"));
		Paginated<Source> p1 = new Paginated<>(sourceList, entriesPerPage, hasNextPage);

		ObjectMapper mapper = new ObjectMapper();
		byte[] json = mapper.writeValueAsBytes(p1);
		Paginated<Source> got = mapper.readValue(json, new TypeReference<Paginated<Source>>() {
		});
		assertEquals(got.getRows().get(0).getN(), "hoge");
	}
	
	public static class Source {
		private String n;

		// dummy constructor
		public Source() {
		}
	
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
