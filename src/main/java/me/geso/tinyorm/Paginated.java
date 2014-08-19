package me.geso.tinyorm;

import java.util.List;

public class Paginated<T> {
	private final List<T> rows;

	private final long currentPage;
	private final long entriesPerPage;
	private final boolean hasNextPage;
	
	public Paginated(List<T> rows, long currentPage, long entriesPerPage, boolean hasNextPage) {
		this.rows = rows;
		this.currentPage = currentPage;
		this.entriesPerPage = entriesPerPage;
		this.hasNextPage = hasNextPage;
	}
	
	public List<T> getRows() {
		return rows;
	}
	public long getCurrentPage() {
		return currentPage;
	}
	public long getEntriesPerPage() {
		return entriesPerPage;
	}
	public boolean hasNextPage() {
		return hasNextPage;
	}
	
}
