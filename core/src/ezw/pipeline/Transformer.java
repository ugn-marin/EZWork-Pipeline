package ezw.pipeline;

import java.util.Collection;
import java.util.Objects;

/**
 * A pipeline worker consuming items from an input pipe, applying a function on them and supplying them for an output
 * supply pipe. The transformation function may return 0 to N output items, thus transforming the index scope of the
 * pipeline workers down the line.
 * @param <I> The input items type.
 * @param <O> The output items type.
 */
public abstract class Transformer<I, O> extends PipelineWorker implements InputComponent<I>, OutputComponent<O> {
    private final Pipe<I> input;
    private final SupplyPipe<O> output;

    /**
     * Creates a single-threaded transformer.
     * @param input The input pipe.
     * @param output The output pipe.
     */
    public Transformer(Pipe<I> input, SupplyPipe<O> output) {
        this(input, output, 1);
    }

    /**
     * Creates a multi-threaded transformer.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param parallel The maximum parallel items transforming to allow.
     */
    public Transformer(Pipe<I> input, SupplyPipe<O> output, int parallel) {
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
            submit(() -> push(transform(indexedItem.getItem())));
        }
        submit(() -> push(conclude()));
    }

    private void push(Collection<O> transformedItems) throws InterruptedException {
        if (transformedItems != null) {
            for (O transformedItem : transformedItems) {
                output.push(transformedItem);
            }
        }
    }

    @Override
    protected void join() throws InterruptedException {
        super.join();
        output.setEndOfInput();
    }

    /**
     * Applies the function on an input item.
     * @param item The input item
     * @return The transformed items collection. If empty or null, skipped, else each output item is pushed into the
     * output pipe.
     * @throws Exception An exception terminating the pipeline.
     */
    protected abstract Collection<O> transform(I item) throws Exception;

    /**
     * Supplies leftover output items when no items left to transform. This would usually only make sense in a single
     * threaded shrinking transformer (returning 0 or 1 outputs per input) with an accumulative logic.
     * @return The transformed items collection. If empty or null, skipped, else each output item is pushed into the
     * output pipe.
     * @throws Exception An exception terminating the pipeline.
     */
    protected abstract Collection<O> conclude() throws Exception;
}
