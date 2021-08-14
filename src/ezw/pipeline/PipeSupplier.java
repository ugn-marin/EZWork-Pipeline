package ezw.pipeline;

import ezw.util.Sugar;
import ezw.util.function.UnsafeSupplier;

import java.util.Objects;

/**
 * A pipeline worker supplying items for a supply pipe.
 * @param <O> The output items type.
 */
public abstract class PipeSupplier<O> extends PipelineWorker implements UnsafeSupplier<O>, SupplyGate<O>,
        OutputWorker<O> {
    private final SupplyPipe<O> output;

    /**
     * Constructs a single-threaded supplier.
     * @param output The output pipe.
     */
    public PipeSupplier(SupplyPipe<O> output) {
        this(output, 1);
    }

    /**
     * Constructs a multi-threaded supplier.
     * @param output The output pipe.
     * @param concurrency The maximum parallel items supplying to allow.
     */
    public PipeSupplier(SupplyPipe<O> output, int concurrency) {
        super(Sugar.requireRange(concurrency, 1, null));
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
        Sugar.repeat(getConcurrency(), () -> submit(() -> Sugar.acceptWhile(this::get, this::push, Objects::nonNull)));
    }

    /**
     * Supplies an item for the output pipe.
     * @return The next item to supply, or null if no more items available.
     * @throws Exception An exception terminating the pipeline.
     */
    public abstract O get() throws Exception;

    @Override
    void internalClose() {
        output.setEndOfInput();
    }

    @Override
    protected String getSimpleName() {
        return "S";
    }
}
