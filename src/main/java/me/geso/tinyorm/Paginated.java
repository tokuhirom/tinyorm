package me.geso.tinyorm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Paginated<T> {
	private final List<T> rows;

	private final long entriesPerPage;
	private final boolean hasNextPage;

	@JsonCreator
	public Paginated(@JsonProperty("rows") List<T> rows,
			@JsonProperty("entriesPerPage") long entriesPerPage,
			@JsonProperty("hasNextPage") boolean hasNextPage) {
		this.rows = rows;
		this.entriesPerPage = entriesPerPage;
		this.hasNextPage = hasNextPage;
	}

	Paginated(final List<T> rows, final long entriesPerPage) {
		if (rows.size() == entriesPerPage + 1) {
			List<T> copied = new ArrayList<>(rows); // copy
			copied.remove(rows.size() - 1); // pop tail
			this.rows = Collections.unmodifiableList(copied);
			this.hasNextPage = true;
		} else {
			this.rows = Collections.unmodifiableList(rows);
			this.hasNextPage = false;
		}
		this.entriesPerPage = entriesPerPage;
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
	 * Create new Paginated instance with row object mapping.<br>
	 * 
	 * {@code paginated.mapRows(row -> row.toJTO());}
	 * 
	 * @param f
	 * @return
	 */
	public <O> Paginated<O> mapRows(Function<T, O> f) {
		List<O> mapped = this.rows.stream().map(f).collect(Collectors.toList());
		return new Paginated<>(mapped, entriesPerPage, hasNextPage);
	}

}
