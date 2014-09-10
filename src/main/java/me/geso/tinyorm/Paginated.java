package me.geso.tinyorm;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Paginated<T> {
	private List<T> rows;

	private long entriesPerPage;
	private boolean hasNextPage;

	@JsonCreator
	public Paginated(@JsonProperty("rows") List<T> rows,
			@JsonProperty("entriesPerPage") long entriesPerPage,
			@JsonProperty("hasNextPage") boolean hasNextPage) {
		this.rows = rows;
		this.entriesPerPage = entriesPerPage;
		this.hasNextPage = hasNextPage;
	}

	public List<T> getRows() {
		return rows;
	}

	public long getEntriesPerPage() {
		return entriesPerPage;
	}

	public boolean getHasNextPage() {
		return hasNextPage;
	}

	/**
	 * Create new Paginated instance with row object mapping.
	 * 
	 * <pre>
	 * <code>
	 * 	paginated.mapRows(row -> row.toJTO());
	 * </code>
	 * </pre>
	 * 
	 * @param f
	 * @return
	 */
	public <O> Paginated<O> mapRows(Function<T, O> f) {
		List<O> mapped = this.rows.stream().map(f).collect(Collectors.toList());
		return new Paginated<>(mapped, entriesPerPage, hasNextPage);
	}

}
