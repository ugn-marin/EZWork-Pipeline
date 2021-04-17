package ezw.pipeline;

import java.util.Objects;

/**
 * A pipeline worker consuming items from an input pipe, applying a function on them and supplying them for an output
 * pipe. The function can create a new index scope if the output pipe is a conditional supply pipe.
 * @param <I> The input items type.
 * @param <O> The output items type.
 */
public abstract class Function<I, O> extends PipelineWorker implements InputComponent<I>, OutputComponent<O> {
    private final Pipe<I> input;
    private final Pipe<O> output;

    /**
     * Creates a single-threaded function.
     * @param input The input pipe.
     * @param output The output pipe.
     */
    public Function(Pipe<I> input, Pipe<O> output) {
        this(input, output, 1);
    }

    /**
     * Creates a multi-threaded function.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param parallel The maximum parallel items applying to allow.
     */
    public Function(Pipe<I> input, Pipe<O> output, int parallel) {
        super(parallel);
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
        for (IndexedItem<I> indexedItem : input) {
            final long index = indexedItem.getIndex();
            final I item = indexedItem.getItem();
            submit(() -> output.push(new IndexedItem<>(index, apply(item), item)));
        }
    }

    @Override
    void join() throws InterruptedException {
        super.join();
        output.setEndOfInput();
    }

    /**
     * Applies the function on an input item.
     * @param item The input item.
     * @return The output item.
     * @throws Exception An exception terminating the pipeline.
     */
    protected abstract O apply(I item) throws Exception;
}
