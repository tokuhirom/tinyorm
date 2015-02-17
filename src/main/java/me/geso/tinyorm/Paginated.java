package me.geso.tinyorm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import me.geso.tinyvalidator.Valid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This is a pager class for {@code SELECT * FROM member WHERE id < ? LIMIT 1000} style pagination.
 * @param <T>
 */
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

	public Paginated(final List<T> rows, final long entriesPerPage) {
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

	/**
	 * Get row objects.
	 * 
	 * @return
	 */
	@Valid
	public List<T> getRows() {
		return rows;
	}

	/**
	 * Get "entriesPerPage".
	 * 
	 * @return
	 */
	public long getEntriesPerPage() {
		return entriesPerPage;
	}

	/**
	 * Return true if the pager has next page. False otherwise.
	 * 
	 * @return
	 */
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
		final List<O> mapped = this.rows.stream().map(f).collect(Collectors.toList());
		return new Paginated<>(mapped, entriesPerPage, hasNextPage);
	}

}
