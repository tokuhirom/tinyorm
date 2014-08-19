package me.geso.tinyorm;

import java.util.List;
import java.util.OptionalLong;

public class Paginated<T> {
	private final List<T> rows;

	private final OptionalLong currentPage;
	private final long entriesPerPage;
	private final boolean hasNextPage;

	public Paginated(List<T> rows, long entriesPerPage, boolean hasNextPage) {
		this.rows = rows;
		this.currentPage = OptionalLong.empty();
		this.entriesPerPage = entriesPerPage;
		this.hasNextPage = hasNextPage;
	}
	
	public Paginated(List<T> rows, long currentPage, long entriesPerPage, boolean hasNextPage) {
		this.rows = rows;
		this.currentPage = OptionalLong.of(currentPage);
		this.entriesPerPage = entriesPerPage;
		this.hasNextPage = hasNextPage;
	}
	
	public List<T> getRows() {
		return rows;
	}
	public OptionalLong getCurrentPage() {
		return currentPage;
	}
	public long getEntriesPerPage() {
		return entriesPerPage;
	}
	public boolean hasNextPage() {
		return hasNextPage;
	}
	
}
