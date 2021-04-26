package ezw.pipeline;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * A queue of items supplied by a pipeline worker.
 * @param <I> The items type.
 */
public class SupplyPipe<I> extends Pipe<I> implements SupplyGate<I> {
    private final AtomicLong index = new AtomicLong();
    private final Predicate<I> predicate;

    /**
     * Constructs a supply pipe.
     * @param baseCapacity The base capacity.
     */
    public SupplyPipe(int baseCapacity) {
        this(baseCapacity, null);
    }

    /**
     * Constructs a conditional supply pipe.
     * @param baseCapacity The base capacity.
     * @param predicate The predicate by which to accept pushed items into the pipe. Ignored if null.
     */
    public SupplyPipe(int baseCapacity, Predicate<I> predicate) {
        super(baseCapacity);
        this.predicate = predicate;
    }

    @Override
    public void push(I item) throws InterruptedException {
        if (predicate == null || predicate.test(item)) {
            super.push(new IndexedItem<>(index.getAndIncrement(), item, true));
        }
    }

    @Override
    void push(IndexedItem<I> indexedItem) throws InterruptedException {
        push(indexedItem.getItem());
    }

    @Override
    public String toString() {
        return String.format("-<S%s:%d>-", predicate != null ? "?" : "", baseCapacity);
    }
}
