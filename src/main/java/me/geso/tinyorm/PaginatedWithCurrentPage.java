package me.geso.tinyorm;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PaginatedWithCurrentPage<T> {
	private List<T> rows;

	private long currentPage;
	private long entriesPerPage;
	private boolean hasNextPage;

	@JsonCreator
	public PaginatedWithCurrentPage(@JsonProperty("rows") List<T> rows, @JsonProperty("currentPage") long currentPage,
			@JsonProperty("entriesPerPage") long entriesPerPage, @JsonProperty("hasNextPage") boolean hasNextPage) {
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