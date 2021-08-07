package ezw.pipeline;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility methods for creating simple pipelines and pipeline workers.
 */
public abstract class Pipelines {

    private Pipelines() {}

    /**
     * Constructs a pipeline from the supplier into the consumer. Equivalent to:
     * <pre>
     * Pipeline.from(supplier).into(consumer);
     * </pre>
     * @param pipeSupplier The supplier.
     * @param pipeConsumer The consumer.
     * @param <O> The items type.
     * @return The pipeline.
     */
    public static <O> Pipeline<O> direct(PipeSupplier<O> pipeSupplier, PipeConsumer<O> pipeConsumer) {
        return Pipeline.from(pipeSupplier).into(pipeConsumer);
    }

    /**
     * Constructs a pipeline from the supplier into the consumer using a simple supply pipe.
     * @param get The supplier get implementation.
     * @param accept The consumer accept implementation.
     * @param <O> The items type.
     * @return The pipeline.
     */
    public static <O> Pipeline<O> direct(Supplier<O> get, Consumer<O> accept) {
        SupplyPipe<O> supplyPipe = new SupplyPipe<>(1);
        return direct(supplier(supplyPipe, get), consumer(supplyPipe, accept));
    }

    /**
     * Constructs a star pipeline forking from the supplier into all the consumers.
     * @param pipeSupplier The supplier.
     * @param pipeConsumers The pipe consumers.
     * @param <O> The items type.
     * @return The pipeline.
     */
    @SafeVarargs
    public static <O> Pipeline<O> star(PipeSupplier<O> pipeSupplier, PipeConsumer<O>... pipeConsumers) {
        return Pipeline.from(pipeSupplier).fork(pipeSupplier, pipeConsumers).into(pipeConsumers);
    }

    /**
     * Constructs an open star pipeline forking from the supply pipe into all the consumers.
     * @param supplyPipe The supply pipe.
     * @param pipeConsumers The pipe consumers.
     * @param <O> The items type.
     * @return The pipeline.
     */
    @SafeVarargs
    public static <O> Pipeline<O> star(SupplyPipe<O> supplyPipe, PipeConsumer<O>... pipeConsumers) {
        return Pipeline.from(supplyPipe).fork(supplyPipe, pipeConsumers).into(pipeConsumers);
    }

    /**
     * Constructs a simple supplier.
     * @param output The output pipe.
     * @param get The get implementation.
     * @param <O> The output items type.
     * @return The supplier.
     */
    public static <O> PipeSupplier<O> supplier(SupplyPipe<O> output, Supplier<O> get) {
        return supplier(output, 1, get);
    }

    /**
     * Constructs a simple multi-threaded supplier.
     * @param output The output pipe.
     * @param parallel The maximum parallel items supplying to allow.
     * @param get The get implementation.
     * @param <O> The output items type.
     * @return The supplier.
     */
    public static <O> PipeSupplier<O> supplier(SupplyPipe<O> output, int parallel, Supplier<O> get) {
        Objects.requireNonNull(get, "Get supplier is required.");
        return new PipeSupplier<>(output, parallel) {

            @Override
            public O get() {
                return get.get();
            }
        };
    }

    /**
     * Constructs a simple function.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param apply The apply implementation.
     * @param <I> The input items type.
     * @param <O> The output items type.
     * @return The function.
     */
    public static <I, O> PipeFunction<I, O> function(Pipe<I> input, Pipe<O> output, Function<I, O> apply) {
        return function(input, output, 1, apply);
    }

    /**
     * Constructs a simple multi-threaded function.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param parallel The maximum parallel items applying to allow.
     * @param apply The apply implementation.
     * @param <I> The input items type.
     * @param <O> The output items type.
     * @return The function.
     */
    public static <I, O> PipeFunction<I, O> function(Pipe<I> input, Pipe<O> output, int parallel,
                                                     Function<I, O> apply) {
        Objects.requireNonNull(apply, "Apply function is required.");
        return new PipeFunction<>(input, output, parallel) {

            @Override
            public O apply(I item) {
                return apply.apply(item);
            }
        };
    }

    /**
     * Constructs a simple transformer.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param apply The apply implementation.
     * @param getLastItems The getLastItems implementation (optional).
     * @param <I> The input items type.
     * @param <O> The output items type.
     * @return The transformer.
     */
    public static <I, O> PipeTransformer<I, O> transformer(Pipe<I> input, SupplyPipe<O> output,
                                                           Function<I, Collection<O>> apply,
                                                           Supplier<Collection<O>> getLastItems) {
        return transformer(input, output, 1, apply, getLastItems);
    }

    /**
     * Constructs a simple multi-threaded transformer.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param parallel The maximum parallel items transforming to allow.
     * @param apply The apply implementation.
     * @param getLastItems The getLastItems implementation (optional).
     * @param <I> The input items type.
     * @param <O> The output items type.
     * @return The transformer.
     */
    public static <I, O> PipeTransformer<I, O> transformer(Pipe<I> input, SupplyPipe<O> output, int parallel,
                                                           Function<I, Collection<O>> apply,
                                                           Supplier<Collection<O>> getLastItems) {
        Objects.requireNonNull(apply, "Apply function is required.");
        return new PipeTransformer<>(input, output, parallel) {

            @Override
            public Collection<O> apply(I item) {
                return apply.apply(item);
            }

            @Override
            protected Collection<O> getLastItems() {
                return getLastItems != null ? getLastItems.get() : null;
            }
        };
    }

    /**
     * Constructs a simple consumer.
     * @param input The input pipe.
     * @param accept The accept implementation.
     * @param <I> The input items type.
     * @return The consumer.
     */
    public static <I> PipeConsumer<I> consumer(Pipe<I> input, Consumer<I> accept) {
        return consumer(input, 1, accept);
    }

    /**
     * Constructs a simple multi-threaded consumer.
     * @param input The input pipe.
     * @param parallel The maximum parallel items consuming to allow.
     * @param accept The accept implementation.
     * @param <I> The input items type.
     * @return The consumer.
     */
    public static <I> PipeConsumer<I> consumer(Pipe<I> input, int parallel, Consumer<I> accept) {
        Objects.requireNonNull(accept, "Accept consumer is required.");
        return new PipeConsumer<>(input, parallel) {

            @Override
            public void accept(I item) {
                accept.accept(item);
            }
        };
    }
}
