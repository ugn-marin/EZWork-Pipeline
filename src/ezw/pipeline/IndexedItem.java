package ezw.pipeline;

/**
 * An item wrapping containing its assigned index in the scope.
 * @param <I> The item type.
 */
final class IndexedItem<I> {
    private final long index;
    private final I item;

    IndexedItem(long index, I item) {
        this.index = index;
        this.item = item;
    }

    long getIndex() {
        return index;
    }

    I getItem() {
        return item;
    }
}
