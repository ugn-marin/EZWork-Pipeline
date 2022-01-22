package ezw.pipeline;

import ezw.concurrent.Interruptible;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * An entry point of items in a new index scope.
 * @param <S> The supplied items type.
 */
@FunctionalInterface
public interface SupplyGate<S> {

    /**
     * Pushes an item using this supply gate's index scope.
     * @param item The item.
     * @throws InterruptedException If interrupted while attempting to push the item.
     */
    void push(S item) throws InterruptedException;

    /**
     * Pushes all the items from the stream into the supply gate.
     * @param stream The stream.
     */
    default void pushAll(Stream<S> stream) {
        stream.forEach(toConsumer());
    }

    /**
     * Wraps this supply gate implementation in an interruptible Consumer.
     */
    default Consumer<S> toConsumer() {
        return item -> Interruptible.accept(this::push, item);
    }
}
