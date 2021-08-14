package ezw.pipeline;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

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
     * @param <I> The items type.
     * @return The pipeline.
     */
    public static <I> Pipeline<I> direct(PipeSupplier<I> pipeSupplier, PipeConsumer<I> pipeConsumer) {
        return Pipeline.from(pipeSupplier).into(pipeConsumer);
    }

    /**
     * Constructs a pipeline from the supplier into the consumer using a simple supply pipe.
     * @param get The supplier get implementation.
     * @param accept The consumer accept implementation.
     * @param <I> The items type.
     * @return The pipeline.
     */
    public static <I> Pipeline<I> direct(Supplier<I> get, Consumer<I> accept) {
        SupplyPipe<I> supplyPipe = new SupplyPipe<>(1);
        return direct(supplier(supplyPipe, get), consumer(supplyPipe, accept));
    }

    /**
     * Constructs a star pipeline forking from the supplier into all the consumers.
     * @param pipeSupplier The supplier.
     * @param pipeConsumers The pipe consumers.
     * @param <I> The items type.
     * @return The pipeline.
     */
    @SafeVarargs
    public static <I> Pipeline<I> star(PipeSupplier<I> pipeSupplier, PipeConsumer<I>... pipeConsumers) {
        return Pipeline.from(pipeSupplier).fork(pipeSupplier, pipeConsumers).into(pipeConsumers);
    }

    /**
     * Constructs an open star pipeline forking from the supply pipe into all the consumers.
     * @param supplyPipe The supply pipe.
     * @param pipeConsumers The pipe consumers.
     * @param <I> The items type.
     * @return The pipeline.
     */
    @SafeVarargs
    public static <I> Pipeline<I> star(SupplyPipe<I> supplyPipe, PipeConsumer<I>... pipeConsumers) {
        return Pipeline.from(supplyPipe).fork(supplyPipe, pipeConsumers).into(pipeConsumers);
    }

    /**
     * Constructs an open star pipeline forking from a supply pipe into the two consumers by the predicate result.
     * @param predicate The predicate by which to accept the items.
     * @param acceptTrue The consumer for items passing the predicate.
     * @param acceptFalse The consumer for items not passing the predicate.
     * @param <I> The items type
     * @return The pipeline.
     */
    public static <I> Pipeline<I> split(Predicate<I> predicate, Consumer<I> acceptTrue, Consumer<I> acceptFalse) {
        return split(Map.of(predicate, acceptTrue, Predicate.not(predicate), acceptFalse));
    }

    /**
     * Constructs an open star pipeline forking from a supply pipe into consumers by predicates' results.
     * @param splitConsumers The predicates mapped to consumers.
     * @param <I> The items type.
     * @return The pipeline.
     */
    @SuppressWarnings("unchecked")
    public static <I> Pipeline<I> split(Map<Predicate<I>, Consumer<I>> splitConsumers) {
        return star(new SupplyPipe<>(1), splitConsumers.entrySet().stream().map(entry -> consumer(
                new SupplyPipe<>(1, entry.getKey()), entry.getValue())).toArray(PipeConsumer[]::new));
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
     * @param concurrency The maximum parallel items supplying to allow.
     * @param get The get implementation.
     * @param <O> The output items type.
     * @return The supplier.
     */
    public static <O> PipeSupplier<O> supplier(SupplyPipe<O> output, int concurrency, Supplier<O> get) {
        Objects.requireNonNull(get, "Get supplier is required.");
        return new PipeSupplier<>(output, concurrency) {

            @Override
            public O get() {
                return get.get();
            }
        };
    }

    /**
     * Constructs a simple supplier of the non-null elements from the stream.
     * @param output The output pipe.
     * @param stream The stream.
     * @param <O> The output items type.
     * @return The supplier.
     */
    public static <O> PipeSupplier<O> supplier(SupplyPipe<O> output, Stream<O> stream) {
        var iterator = Objects.requireNonNull(stream, "Stream is null.").filter(Objects::nonNull).iterator();
        return supplier(output, () -> iterator.hasNext() ? iterator.next() : null);
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
     * @param concurrency The maximum parallel items applying to allow.
     * @param apply The apply implementation.
     * @param <I> The input items type.
     * @param <O> The output items type.
     * @return The function.
     */
    public static <I, O> PipeFunction<I, O> function(Pipe<I> input, Pipe<O> output, int concurrency,
                                                     Function<I, O> apply) {
        Objects.requireNonNull(apply, "Apply function is required.");
        return new PipeFunction<>(input, output, concurrency) {

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
     * @param concurrency The maximum parallel items transforming to allow.
     * @param apply The apply implementation.
     * @param getLastItems The getLastItems implementation (optional).
     * @param <I> The input items type.
     * @param <O> The output items type.
     * @return The transformer.
     */
    public static <I, O> PipeTransformer<I, O> transformer(Pipe<I> input, SupplyPipe<O> output, int concurrency,
                                                           Function<I, Collection<O>> apply,
                                                           Supplier<Collection<O>> getLastItems) {
        Objects.requireNonNull(apply, "Apply function is required.");
        return new PipeTransformer<>(input, output, concurrency) {

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
     * @param concurrency The maximum parallel items consuming to allow.
     * @param accept The accept implementation.
     * @param <I> The input items type.
     * @return The consumer.
     */
    public static <I> PipeConsumer<I> consumer(Pipe<I> input, int concurrency, Consumer<I> accept) {
        Objects.requireNonNull(accept, "Accept consumer is required.");
        return new PipeConsumer<>(input, concurrency) {

            @Override
            public void accept(I item) {
                accept.accept(item);
            }
        };
    }
}
