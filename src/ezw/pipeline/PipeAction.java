package ezw.pipeline;

import ezw.function.UnsafeConsumer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A pipe function acting upon items from an input pipe, and passing them to an output pipe as soon as done. Essentially
 * an identity function with a side effect.
 * @param <I> The items type.
 */
public abstract class PipeAction<I> extends PipeFunction<I, I> implements UnsafeConsumer<I> {
    private final AtomicBoolean isEnabled = new AtomicBoolean(true);

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

    /**
     * Returns true if the action is enabled, else false.
     */
    public boolean isEnabled() {
        return isEnabled.get();
    }

    /**
     * Updates the action's enabled status.
     * @param isEnabled The new enabled status.
     * @return The previous enabled status.
     */
    public boolean setEnabled(boolean isEnabled) {
        return this.isEnabled.getAndSet(isEnabled);
    }

    @Override
    public I apply(I item) throws Exception {
        if (isEnabled())
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
