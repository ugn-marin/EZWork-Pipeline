package ezw.pipeline;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
     * Pipeline.from(supplier).into(consumer).build();
     * </pre>
     * @param pipeSupplier The supplier.
     * @param pipeConsumer The consumer.
     * @param <I> The items type.
     * @return The pipeline.
     */
    public static <I> Pipeline<I> direct(PipeSupplier<I> pipeSupplier, PipeConsumer<I> pipeConsumer) {
        return Pipeline.from(pipeSupplier).into(pipeConsumer).build();
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
     * Constructs a pipeline from a simple supplier of the non-null elements from the stream, into the consumer.
     * @param stream The stream.
     * @param pipeConsumer The consumer.
     * @param <I> The items type.
     * @return The pipeline.
     */
    public static <I> Pipeline<I> direct(Stream<I> stream, PipeConsumer<I> pipeConsumer) {
        if (!(Objects.requireNonNull(pipeConsumer, "Consumer is null.").getInput() instanceof SupplyPipe))
            throw new PipelineConfigurationException("The direct pipeline consumer input pipe must be a supply pipe.");
        return direct(supplier((SupplyPipe<I>) pipeConsumer.getInput(), stream), pipeConsumer);
    }

    /**
     * Constructs a pipeline from a simple supplier of the non-null elements from the stream, into a simple
     * multithreaded consumer. Similar to (plus the pipeline functionality of cancellation, concurrency control etc.):
     * <pre>
     * stream.filter(Objects::nonNull).parallel().forEach(accept);
     * </pre>
     * @param stream The stream.
     * @param concurrency The maximum parallel items consuming to allow.
     * @param accept The consumer accept implementation.
     * @param <I> The items type.
     * @return The pipeline.
     */
    public static <I> Pipeline<I> direct(Stream<I> stream, int concurrency, Consumer<I> accept) {
        SupplyPipe<I> supplyPipe = new SupplyPipe<>(concurrency);
        return direct(supplier(supplyPipe, stream), consumer(supplyPipe, concurrency, accept));
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
        return Pipeline.from(pipeSupplier).fork(pipeSupplier, pipeConsumers).into(pipeConsumers).build();
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
        return Pipeline.from(supplyPipe).fork(supplyPipe, pipeConsumers).into(pipeConsumers).build();
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
     * Constructs a simple pipe supplier.
     * @param output The output pipe.
     * @param get The get implementation.
     * @param <O> The output items type.
     * @return The supplier.
     */
    public static <O> PipeSupplier<O> supplier(SupplyPipe<O> output, Supplier<O> get) {
        return supplier(output, 1, get);
    }

    /**
     * Constructs a simple multithreaded pipe supplier.
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
            public Optional<O> get() {
                return Optional.ofNullable(get.get());
            }
        };
    }

    /**
     * Constructs a simple pipe supplier of the non-null elements from the stream.
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
     * Constructs a pipe supplier of a single non-null object.
     * @param object The object to supply.
     * @param <O> The object type.
     * @return The supplier.
     */
    public static <O> PipeSupplier<O> supplier(O object) {
        return supplier(new SupplyPipe<>(1), Stream.of(Objects.requireNonNull(object, "Object is null.")));
    }

    /**
     * Constructs a simple pipe function.
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
     * Constructs a simple multithreaded pipe function.
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
     * Constructs a simple pipe action.
     * @param accept The accept implementation.
     * @param <I> The items type.
     * @return The action.
     */
    public static <I> PipeAction<I> action(Consumer<I> accept) {
        return action(1, accept);
    }

    /**
     * Constructs a simple multithreaded pipe action.
     * @param concurrency The maximum parallel items accepting to allow.
     * @param accept The accept implementation.
     * @param <I>  The items type.
     * @return The action.
     */
    public static <I> PipeAction<I> action(int concurrency, Consumer<I> accept) {
        return action(new IndexedPipe<>(concurrency), new IndexedPipe<>(concurrency), concurrency, accept);
    }

    /**
     * Constructs a simple pipe action.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param accept The accept implementation.
     * @param <I> The items type.
     * @return The action.
     */
    public static <I> PipeAction<I> action(Pipe<I> input, Pipe<I> output, Consumer<I> accept) {
        return action(input, output, 1, accept);
    }

    /**
     * Constructs a simple multithreaded pipe action.
     * @param input The input pipe.
     * @param output The output pipe.
     * @param concurrency The maximum parallel items accepting to allow.
     * @param accept The accept implementation.
     * @param <I> The items type.
     * @return The action.
     */
    public static <I> PipeAction<I> action(Pipe<I> input, Pipe<I> output, int concurrency, Consumer<I> accept) {
        Objects.requireNonNull(accept, "Accept consumer is required.");
        return new PipeAction<>(input, output, concurrency) {

            @Override
            public void accept(I item) {
                accept.accept(item);
            }
        };
    }

    /**
     * Constructs a simple pipe transformer.
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
     * Constructs a simple multithreaded pipe transformer.
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
     * Constructs a simple pipe consumer.
     * @param accept The accept implementation.
     * @param <I> The input items type.
     * @return The consumer.
     */
    public static <I> PipeConsumer<I> consumer(Consumer<I> accept) {
        return consumer(1, accept);
    }

    /**
     * Constructs a simple multithreaded pipe consumer.
     * @param concurrency The maximum parallel items consuming to allow.
     * @param accept The accept implementation.
     * @param <I> The input items type.
     * @return The consumer.
     */
    public static <I> PipeConsumer<I> consumer(int concurrency, Consumer<I> accept) {
        return consumer(new IndexedPipe<>(concurrency), concurrency, accept);
    }

    /**
     * Constructs a simple pipe consumer.
     * @param input The input pipe.
     * @param accept The accept implementation.
     * @param <I> The input items type.
     * @return The consumer.
     */
    public static <I> PipeConsumer<I> consumer(Pipe<I> input, Consumer<I> accept) {
        return consumer(input, 1, accept);
    }

    /**
     * Constructs a simple multithreaded pipe consumer.
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
