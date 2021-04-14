package ezw.pipeline;

/**
 * An item wrapping, containing its assigned index and a modification flag indicating that the item is a new value,
 * either just created or modified by a worker.
 * @param <I> The item type.
 */
final class IndexedItem<I> {
    private final long index;
    private final I item;
    private final boolean isModified;

    IndexedItem(long index, I item, Object previousItem) {
        this(index, item, item != previousItem);
    }

    IndexedItem(long index, I item, boolean isModified) {
        this.index = index;
        this.item = item;
        this.isModified = isModified;
    }

    long getIndex() {
        return index;
    }

    I getItem() {
        return item;
    }

    boolean isModified() {
        return isModified;
    }
}
