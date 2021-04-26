package ezw.pipeline;

import ezw.util.Sugar;

import java.util.Arrays;
import java.util.Objects;

/**
 * A pipeline worker sending the input item reference to several output pipes simultaneously. The fork can create a new
 * index scope for any output pipe that is a conditional supply pipe. Every supply pipe is pushed into synchronously,
 * potentially blocking other available pipes pushing.
 * @param <I> The items type.
 */
final class Fork<I> extends PipelineWorker implements InputComponent<I> {
    private final Pipe<I> input;
    private final Pipe<I>[] outputs;

    @SafeVarargs
    Fork(Pipe<I> input, Pipe<I>... outputs) {
        super(Math.max(1, (int) Arrays.stream(Sugar.Collections.requireNoneNull(outputs)).filter(
                p -> !(p instanceof SupplyPipe)).count()));
        if (outputs.length < 2)
            throw new IllegalArgumentException("Fork requires at least 2 output pipes.");
        this.input = Objects.requireNonNull(input, "Input pipe is required.");
        this.outputs = outputs;
    }

    @Override
    public Pipe<I> getInput() {
        return input;
    }

    Pipe<I>[] getOutputs() {
        return outputs;
    }

    @Override
    protected void work() throws InterruptedException {
        for (IndexedItem<I> indexedItem : input) {
            for (Pipe<I> output : outputs) {
                if (output instanceof SupplyPipe)
                    output.push(indexedItem);
                else
                    submit(() -> output.push(indexedItem));
            }
        }
    }

    @Override
    protected void join() throws InterruptedException {
        super.join();
        Arrays.stream(outputs).forEach(Pipe::setEndOfInput);
    }

    @Override
    public String toString() {
        return "fork";
    }
}
