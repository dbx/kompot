package hu.dbx.kompot.report;

/**
 * Contains information about pagination of result set. Specifies offset and page size.
 */
public final class Pagination {

    private final int offset, limit;

    /**
     * Constructs new pagination instance for offset and page size. Throws when impossible.
     *
     * @param from  offset must not be negative. Starts from zero.
     * @param limit page size must be positive.
     * @return new pagionation instance.
     */
    public static Pagination fromOffsetAndLimit(int from, int limit) {
        if (from < 0) {
            throw new IllegalArgumentException("Page offset must not be negative: " + from);
        } else if (limit <= 0) {
            throw new IllegalArgumentException("Page size must be positive: " + limit);
        } else {
            return new Pagination(from, limit);
        }
    }

    private Pagination(int from, int limit) {
        this.offset = from;
        this.limit = limit;
    }
    
    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }
}
