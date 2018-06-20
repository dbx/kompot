package hu.dbx.kompot.report;

@SuppressWarnings("unused")
class ListResult<X> {

    private int from;
    private int limit;
    private int total;
    private Iterable<X> items;


    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public Iterable<X> getItems() {
        return items;
    }

    public void setItems(Iterable<X> items) {
        this.items = items;
    }
}
