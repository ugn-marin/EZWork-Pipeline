package ezw.pipeline;

/**
 * A queue of items moved between pipeline workers, preserving the index scope.
 * @param <I> The items type.
 */
public class IndexedPipe<I> extends Pipe<I> {

    /**
     * Constructs an indexed pipe.
     * @param baseCapacity The base capacity (<code>BC</code>) of the pipe. Used as the capacity for the in-order queue,
     *                     as well as the out-of-order items cache. Together with the in-push items, which depends on
     *                     the number of the pushing threads <code>N</code>, the total maximum theoretical capacity of
     *                     the pipe can reach <code>BC+N</code>.
     */
    public IndexedPipe(int baseCapacity) {
        this(baseCapacity, "IP");
    }

    /**
     * Constructs an indexed pipe.
     * @param baseCapacity The base capacity (<code>BC</code>) of the pipe. Used as the capacity for the in-order queue,
     *                     as well as the out-of-order items cache. Together with the in-push items, which depends on
     *                     the number of the pushing threads <code>N</code>, the total maximum theoretical capacity of
     *                     the pipe can reach <code>BC+N</code>.
     * @param name The name of the pipe.
     */
    public IndexedPipe(int baseCapacity, String name) {
        super(baseCapacity, name);
    }
}
