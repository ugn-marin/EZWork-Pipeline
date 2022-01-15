package ezw.pipeline;

/**
 * An item wrapping containing its assigned index in the scope.
 * @param <I> The item type.
 */
record IndexedItem<I>(long index, I item) {}
