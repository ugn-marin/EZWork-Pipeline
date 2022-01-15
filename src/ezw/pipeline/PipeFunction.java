package ezw.pipeline;

import ezw.Sugar;
import ezw.function.UnsafeFunction;

import java.util.Objects;

/**
 * A pipeline worker consuming items from an input pipe, applying a function on them and supplying them for an output
 * pipe.
 * @param <I> The input items type.
 * @param <O> The output items type.
 */
public abstract class PipeFunction<I, O> extends PipelineWorker implements UnsafeFunction<I, O>, InputWorker<I>,
        OutputWorker<O> {
    private final Pipe<I> input;
    private final Pipe<O> output;

    /**
     * Constructs a single-threaded function.
     * @param input The input pipe.
     * @param output The output pipe.
     */
    public PipeFunction(Pipe<I> input, Pipe<O> output) {
        this(input, output, 1);
    }

    /**
     * Constructs a multithreaded function.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param concurrency The maximum parallel items applying to allow.
     */
    public PipeFunction(Pipe<I> input, Pipe<O> output, int concurrency) {
        super(false, Sugar.requireRange(concurrency, 1, null));
        this.input = Objects.requireNonNull(input, "Input pipe is required.");
        this.output = Objects.requireNonNull(output, "Output pipe is required.");
    }

    @Override
    public Pipe<I> getInput() {
        return input;
    }

    @Override
    public Pipe<O> getOutput() {
        return output;
    }

    @Override
    protected void work() {
        input.forEachRemaining(indexedItem -> submit(() -> output.push(new IndexedItem<>(indexedItem.index(),
                apply(indexedItem.item())))));
    }

    /**
     * Applies the function on an input item.
     * @param item The input item.
     * @return The output item.
     * @throws Exception An exception terminating the pipeline.
     */
    public abstract O apply(I item) throws Exception;

    @Override
    void internalClose() {
        output.setEndOfInput();
    }
}
