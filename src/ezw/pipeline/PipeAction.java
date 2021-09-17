package ezw.pipeline;

import ezw.util.function.UnsafeConsumer;

/**
 * A pipe function acting upon items from an input pipe, and passing them to an output pipe as soon as done.
 * @param <I> The items type.
 */
public abstract class PipeAction<I> extends PipeFunction<I, I> implements UnsafeConsumer<I> {

    /**
     * Constructs a single-threaded action.
     * @param input The input pipe.
     * @param output The output pipe.
     */
    public PipeAction(Pipe<I> input, Pipe<I> output) {
        super(input, output);
    }

    /**
     * Constructs a multi-threaded action.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param concurrency The maximum parallel items accepting to allow.
     */
    public PipeAction(Pipe<I> input, Pipe<I> output, int concurrency) {
        super(input, output, concurrency);
    }

    @Override
    public I apply(I item) throws Exception {
        accept(item);
        return item;
    }

    /**
     * Accepts an item from the input pipe.
     * @param item The item.
     * @throws Exception An exception terminating the pipeline.
     */
    public abstract void accept(I item) throws Exception;
}
