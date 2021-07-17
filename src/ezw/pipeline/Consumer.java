package ezw.pipeline;

import ezw.util.Sugar;
import ezw.util.function.UnsafeConsumer;

import java.util.Objects;

/**
 * A pipeline worker consuming items from a pipe.
 * @param <I> The input items type.
 */
public abstract class Consumer<I> extends PipelineWorker implements UnsafeConsumer<I>, InputComponent<I> {
    private final Pipe<I> input;

    /**
     * Constructs a single-threaded consumer.
     * @param input The input pipe.
     */
    public Consumer(Pipe<I> input) {
        this(input, 1);
    }

    /**
     * Constructs a multi-threaded consumer.
     * @param input The input pipe.
     * @param parallel The maximum parallel items consuming to allow.
     */
    public Consumer(Pipe<I> input, int parallel) {
        super(Sugar.requireRange(parallel, 1, null));
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
