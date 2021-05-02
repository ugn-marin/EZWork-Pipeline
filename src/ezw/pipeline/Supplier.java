package ezw.pipeline;

import java.util.Objects;

/**
 * A pipeline worker supplying items for a supply pipe.
 * @param <O> The output items type.
 */
public abstract class Supplier<O> extends PipelineWorker implements SupplyGate<O>, OutputComponent<O> {
    private final SupplyPipe<O> output;

    /**
     * Constructs a single-threaded supplier.
     * @param output The output pipe.
     */
    public Supplier(SupplyPipe<O> output) {
        this(output, 1);
    }

    /**
     * Constructs a multi-threaded supplier.
     * @param output The output pipe.
     * @param parallel The maximum parallel items supplying to allow.
     */
    public Supplier(SupplyPipe<O> output, int parallel) {
        super(parallel);
        this.output = Objects.requireNonNull(output, "Output pipe is required.");
    }

    @Override
    public Pipe<O> getOutput() {
        return output;
    }

    @Override
    public void push(O item) throws InterruptedException {
        output.push(item);
    }

    @Override
    protected void work() {
        for (int i = 0; i < getParallel(); i++) {
            submit(() -> {
                O item = get();
                while (item != null) {
                    push(item);
                    item = get();
                }
            });
        }
    }

    @Override
    protected void join() throws InterruptedException {
        super.join();
        output.setEndOfInput();
    }

    /**
     * Supplies an item for the output pipe.
     * @return The next item to supply, or null if no more items available.
     * @throws Exception An exception terminating the pipeline.
     */
    protected abstract O get() throws Exception;
}
