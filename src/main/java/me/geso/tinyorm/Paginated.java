package me.geso.tinyorm;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtils;

public class Paginated<T> {
	private final List<T> rows;

	private final long entriesPerPage;
	private final boolean hasNextPage;

	public Paginated(List<T> rows, long entriesPerPage, boolean hasNextPage) {
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

	public <O> Paginated<O> mapRows(Function<T, O> f) {
		List<O> mapped = this.rows.stream().map(f).collect(Collectors.toList());
		return new Paginated<>(mapped, entriesPerPage, hasNextPage);
	}

	public <O> Paginated<O> mapToBean(Class<O> klass) {
		List<O> mapped = this.rows.stream().map(row -> {
			try {
				O instance = klass.newInstance();
				BeanUtils.copyProperties(instance, row);
				return instance;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}).collect(Collectors.toList());
		return new Paginated<>(mapped, entriesPerPage, hasNextPage);
	}

}
