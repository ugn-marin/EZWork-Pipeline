package ezw.pipeline;

import ezw.util.Sugar;
import ezw.util.function.UnsafeFunction;

import java.util.Collection;
import java.util.Objects;

/**
 * A pipeline worker consuming items from an input pipe, applying a function on them and supplying them for an output
 * supply pipe. The transformation function may return 0 to N output items, thus transforming the index scope of the
 * pipeline workers down the line.
 * @param <I> The input items type.
 * @param <O> The output items type.
 */
public abstract class PipeTransformer<I, O> extends PipelineWorker implements UnsafeFunction<I, Collection<O>>,
        InputWorker<I>, OutputWorker<O> {
    private final Pipe<I> input;
    private final SupplyPipe<O> output;

    /**
     * Constructs a single-threaded transformer.
     * @param input The input pipe.
     * @param output The output pipe.
     */
    public PipeTransformer(Pipe<I> input, SupplyPipe<O> output) {
        this(input, output, 1);
    }

    /**
     * Constructs a multi-threaded transformer.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param concurrency The maximum parallel items transforming to allow.
     */
    public PipeTransformer(Pipe<I> input, SupplyPipe<O> output, int concurrency) {
        super(Sugar.requireRange(concurrency, 1, null));
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
            submit(() -> push(apply(indexedItem.getItem())));
        }
        submit(() -> push(getLastItems()));
    }

    private void push(Collection<O> transformedItems) throws InterruptedException {
        if (transformedItems != null) {
            for (O transformedItem : transformedItems) {
                output.push(transformedItem);
            }
        }
    }

    /**
     * Applies the function on an input item.
     * @param item The input item
     * @return The transformed items. If empty or null - skipped, else each output item is pushed into the output pipe.
     * @throws Exception An exception terminating the pipeline.
     */
    public abstract Collection<O> apply(I item) throws Exception;

    /**
     * Supplies leftover output items when no items left to transform. This would usually only make sense in a shrinking
     * transformer (returning 0 or 1 outputs per input) with an accumulative logic.
     * @return The transformed items left to return at the end of input. If empty or null - skipped, else each output
     * item is pushed into the output pipe.
     * @throws Exception An exception terminating the pipeline.
     */
    protected abstract Collection<O> getLastItems() throws Exception;

    @Override
    void internalClose() {
        output.setEndOfInput();
    }

    @Override
    protected String getSimpleName() {
        return "T";
    }
}
