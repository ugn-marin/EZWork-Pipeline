package ezw.pipeline;

/**
 * An entry point of items in a new index scope.
 * @param <S> The supplied items type.
 */
public interface SupplyGate<S> {

    /**
     * Pushes an item using this supply gate's index scope.
     * @param item The item.
     * @throws InterruptedException If interrupted while attempting to push the item.
     */
    void push(S item) throws InterruptedException;
}
