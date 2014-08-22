package me.geso.tinyorm;

import java.util.List;

public class PaginatedWithCurrentPage<T> {
	private List<T> rows;

	private long currentPage;
	private long entriesPerPage;
	private boolean hasNextPage;

	public PaginatedWithCurrentPage() {
		this.rows = null;
		this.currentPage = 0;
		this.entriesPerPage = 0;
		this.hasNextPage = false;
	}

	public PaginatedWithCurrentPage(List<T> rows, long currentPage,
			long entriesPerPage, boolean hasNextPage) {
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

	public boolean getHasNextPage() {
		return hasNextPage;
	}

}