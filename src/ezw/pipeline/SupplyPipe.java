package ezw.pipeline;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * A queue of items supplied by a pipeline worker, and/or from the outside.
 * @param <I> The items type.
 */
public class SupplyPipe<I> extends Pipe<I> implements SupplyGate<I> {
    private final AtomicLong index = new AtomicLong();
    private final Predicate<I> predicate;

    /**
     * Constructs a supply pipe.
     * @param baseCapacity The base capacity (<code>BC</code>) of the pipe. Used as the capacity for the in-order queue,
     *                     as well as the out-of-order items cache. Together with the in-push items, which depends on
     *                     the number of the pushing threads <code>N</code>, the total maximum theoretical capacity of
     *                     the pipe can reach <code>BC+N</code>.
     */
    public SupplyPipe(int baseCapacity) {
        this(baseCapacity, (Predicate<I>) null);
    }

    /**
     * Constructs a supply pipe.
     * @param baseCapacity The base capacity (<code>BC</code>) of the pipe. Used as the capacity for the in-order queue,
     *                     as well as the out-of-order items cache. Together with the in-push items, which depends on
     *                     the number of the pushing threads <code>N</code>, the total maximum theoretical capacity of
     *                     the pipe can reach <code>BC+N</code>.
     * @param name The name of the pipe.
     */
    public SupplyPipe(int baseCapacity, String name) {
        this(baseCapacity, name, null);
    }

    /**
     * Constructs a conditional supply pipe.
     * @param baseCapacity The base capacity (<code>BC</code>) of the pipe. Used as the capacity for the in-order queue,
     *                     as well as the out-of-order items cache. Together with the in-push items, which depends on
     *                     the number of the pushing threads <code>N</code>, the total maximum theoretical capacity of
     *                     the pipe can reach <code>BC+N</code>.
     * @param predicate The predicate by which to accept pushed items into the pipe. Ignored if null.
     */
    public SupplyPipe(int baseCapacity, Predicate<I> predicate) {
        this(baseCapacity, String.format("S%sP", predicate != null ? "?" : ""), predicate);
    }

    /**
     * Constructs a conditional supply pipe.
     * @param baseCapacity The base capacity (<code>BC</code>) of the pipe. Used as the capacity for the in-order queue,
     *                     as well as the out-of-order items cache. Together with the in-push items, which depends on
     *                     the number of the pushing threads <code>N</code>, the total maximum theoretical capacity of
     *                     the pipe can reach <code>BC+N</code>.
     * @param name The name of the pipe.
     * @param predicate The predicate by which to accept pushed items into the pipe. Ignored if null.
     */
    public SupplyPipe(int baseCapacity, String name, Predicate<I> predicate) {
        super(baseCapacity, name);
        this.predicate = predicate;
    }

    @Override
    public void push(I item) throws InterruptedException {
        if (predicate == null || predicate.test(item))
            super.push(new IndexedItem<>(index.getAndIncrement(), item));
    }

    @Override
    void push(IndexedItem<I> indexedItem) throws InterruptedException {
        push(indexedItem.getItem());
    }
}
