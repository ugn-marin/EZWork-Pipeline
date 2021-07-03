package ezw.pipeline;

import java.util.Collection;
import java.util.Objects;

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
     * @param supplier The supplier.
     * @param consumer The consumer.
     * @param <O> The items type.
     * @return The pipeline.
     */
    public static <O> Pipeline<O> direct(Supplier<O> supplier, Consumer<O> consumer) {
        return Pipeline.from(supplier).into(consumer);
    }

    /**
     * Constructs a pipeline from the supplier into the consumer using a simple supply pipe.
     * @param get The supplier get implementation.
     * @param accept The consumer accept implementation.
     * @param <O> The items type.
     * @return The pipeline.
     */
    public static <O> Pipeline<O> direct(java.util.function.Supplier<O> get, java.util.function.Consumer<O> accept) {
        SupplyPipe<O> supplyPipe = new SupplyPipe<>(1);
        return direct(supplier(supplyPipe, get), consumer(supplyPipe, accept));
    }

    /**
     * Constructs a star pipeline forking from the supplier into all the consumers.
     * @param supplier The supplier.
     * @param consumers The consumers.
     * @param <O> The items type.
     * @return The pipeline.
     */
    @SafeVarargs
    public static <O> Pipeline<O> star(Supplier<O> supplier, Consumer<O>... consumers) {
        return Pipeline.from(supplier).fork(supplier, consumers).into(consumers);
    }

    /**
     * Constructs an open star pipeline forking from the supply pipe into all the consumers.
     * @param supplyPipe The supply pipe.
     * @param consumers The consumers.
     * @param <O> The items type.
     * @return The pipeline.
     */
    @SafeVarargs
    public static <O> Pipeline<O> star(SupplyPipe<O> supplyPipe, Consumer<O>... consumers) {
        return Pipeline.from(supplyPipe).fork(supplyPipe, consumers).into(consumers);
    }

    /**
     * Constructs a simple supplier.
     * @param output The output pipe.
     * @param get The get implementation.
     * @param <O> The output items type.
     * @return The supplier.
     */
    public static <O> Supplier<O> supplier(SupplyPipe<O> output, java.util.function.Supplier<O> get) {
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
    public static <O> Supplier<O> supplier(SupplyPipe<O> output, int parallel, java.util.function.Supplier<O> get) {
        Objects.requireNonNull(get, "Get supplier is required.");
        return new Supplier<>(output, parallel) {

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
    public static <I, O> Function<I, O> function(Pipe<I> input, Pipe<O> output,
                                                 java.util.function.Function<I, O> apply) {
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
    public static <I, O> Function<I, O> function(Pipe<I> input, Pipe<O> output, int parallel,
                                                 java.util.function.Function<I, O> apply) {
        Objects.requireNonNull(apply, "Apply function is required.");
        return new Function<>(input, output, parallel) {

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
     * @param conclude The conclude implementation (optional).
     * @param <I> The input items type.
     * @param <O> The output items type.
     * @return The transformer.
     */
    public static <I, O> Transformer<I, O> transformer(Pipe<I> input, SupplyPipe<O> output,
                                                       java.util.function.Function<I, Collection<O>> apply,
                                                       java.util.function.Supplier<Collection<O>> conclude) {
        return transformer(input, output, 1, apply, conclude);
    }

    /**
     * Constructs a simple multi-threaded transformer.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param parallel The maximum parallel items transforming to allow.
     * @param apply The apply implementation.
     * @param conclude The conclude implementation (optional).
     * @param <I> The input items type.
     * @param <O> The output items type.
     * @return The transformer.
     */
    public static <I, O> Transformer<I, O> transformer(Pipe<I> input, SupplyPipe<O> output, int parallel,
                                                       java.util.function.Function<I, Collection<O>> apply,
                                                       java.util.function.Supplier<Collection<O>> conclude) {
        Objects.requireNonNull(apply, "Apply function is required.");
        return new Transformer<>(input, output, parallel) {

            @Override
            public Collection<O> apply(I item) {
                return apply.apply(item);
            }

            @Override
            protected Collection<O> conclude() {
                return conclude != null ? conclude.get() : null;
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
    public static <I> Consumer<I> consumer(Pipe<I> input, java.util.function.Consumer<I> accept) {
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
    public static <I> Consumer<I> consumer(Pipe<I> input, int parallel, java.util.function.Consumer<I> accept) {
        Objects.requireNonNull(accept, "Accept consumer is required.");
        return new Consumer<>(input, parallel) {

            @Override
            public void accept(I item) {
                accept.accept(item);
            }
        };
    }
}
