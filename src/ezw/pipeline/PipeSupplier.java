package ezw.pipeline;

import ezw.Sugar;
import ezw.function.UnsafeSupplier;

import java.util.Objects;
import java.util.function.Predicate;

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
    protected void work() {
        Sugar.repeat(getConcurrency(), () -> submit(() -> Sugar.acceptWhile(this::get, this::push,
                Predicate.not(this::isTerminator))));
    }

    /**
     * Supplies an item for the output pipe.
     * @return The next item to supply, or the terminator value if no more items available.
     * @throws Exception An exception terminating the pipeline.
     */
    public abstract O get() throws Exception;

    /**
     * Returns true if the item is the <i>terminator</i> value - a value signaling the end of input from the supplier.
     * The terminator value is not pushed into the supply pipe. The default terminator value is null.
     */
    protected boolean isTerminator(O item) {
        return item == null;
    }

    @Override
    void internalClose() {
        output.setEndOfInput();
    }
}
