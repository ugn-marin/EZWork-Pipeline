package ezw.pipeline;

import ezw.Sugar;
import ezw.function.UnsafeSupplier;

import java.util.Objects;
import java.util.Optional;

/**
 * A pipeline worker supplying items for a supply pipe.
 * @param <O> The output items type.
 */
public abstract class PipeSupplier<O> extends PipelineWorker implements UnsafeSupplier<Optional<O>>, SupplyGate<O>,
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
     * Constructs a multithreaded supplier.
     * @param output The output pipe.
     * @param concurrency The maximum parallel items supplying to allow.
     */
    public PipeSupplier(SupplyPipe<O> output, int concurrency) {
        super(false, Sugar.requireRange(concurrency, 1, null));
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
    void work() {
        Sugar.repeat(getConcurrency(), () -> submit(() -> Sugar.acceptWhilePresent(() -> busyGet(this), this::push)));
    }

    /**
     * Supplies an optional item for the output pipe.
     * @return An optional of the next item to supply, or empty if no more items available.
     * @throws Exception An exception terminating the pipeline.
     */
    public abstract Optional<O> get() throws Exception;

    @Override
    void internalClose() {
        output.setEndOfInput();
    }
}
