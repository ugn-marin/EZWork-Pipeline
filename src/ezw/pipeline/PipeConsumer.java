package ezw.pipeline;

import ezw.Sugar;
import ezw.function.UnsafeConsumer;

import java.util.Objects;

/**
 * A pipeline worker consuming items from a pipe.
 * @param <I> The input items type.
 */
public abstract class PipeConsumer<I> extends PipelineWorker implements UnsafeConsumer<I>, InputWorker<I> {
    private final Pipe<I> input;

    /**
     * Constructs a single-threaded consumer.
     * @param input The input pipe.
     */
    public PipeConsumer(Pipe<I> input) {
        this(input, 1);
    }

    /**
     * Constructs a multi-threaded consumer.
     * @param input The input pipe.
     * @param concurrency The maximum parallel items consuming to allow.
     */
    public PipeConsumer(Pipe<I> input, int concurrency) {
        super(Sugar.requireRange(concurrency, 1, null));
        this.input = Objects.requireNonNull(input, "Input pipe is required.");
    }

    @Override
    public Pipe<I> getInput() {
        return input;
    }

    @Override
    protected void work() {
        for (IndexedItem<I> indexedItem : input) {
            submit(() -> accept(indexedItem.getItem()));
        }
    }

    /**
     * Consumes an item from the input pipe.
     * @param item The item.
     * @throws Exception An exception terminating the pipeline.
     */
    public abstract void accept(I item) throws Exception;
}
