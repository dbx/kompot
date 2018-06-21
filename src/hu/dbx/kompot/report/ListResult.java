package hu.dbx.kompot.report;

import java.util.Iterator;

final class ListResult<T> implements Iterable<T> {

    private final int from;
    private final int limit;
    private final int total;
    private final Iterable<T> items;

    ListResult(int from, int limit, int total, Iterable<T> items) {
        this.from = from;
        this.limit = limit;
        this.total = total;
        this.items = items;
    }

    public int getOffset() {
        return from;
    }

    public int getLimit() {
        return limit;
    }

    public int getTotal() {
        return total;
    }

    @Override
    public Iterator<T> iterator() {
        return items.iterator();
    }
}