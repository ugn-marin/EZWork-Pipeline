package ezw.pipeline;

import ezw.Sugar;

import java.util.Objects;
import java.util.stream.Stream;

/**
 * An internal worker sending the input item reference to several output pipes simultaneously. The fork can create a new
 * index scope for any output pipe that is a conditional supply pipe. Every supply pipe is pushed into synchronously to
 * enforce the items order, potentially blocking other available pipes pushing.
 * @param <I> The items type.
 */
final class Fork<I> extends PipelineWorker implements InputWorker<I> {
    private final Pipe<I> input;
    private final Pipe<I>[] outputs;

    @SafeVarargs
    Fork(Pipe<I> input, Pipe<I>... outputs) {
        super(true, (int) Stream.of(Sugar.requireNoneNull(outputs)).filter(p -> !(p instanceof SupplyGate)).count());
        if (outputs.length < 2)
            throw new PipelineConfigurationException("Fork requires at least 2 output pipes.");
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
    void work() throws InterruptedException {
        input.drain(indexedItem -> {
            for (var output : outputs) {
                if (output instanceof SupplyGate)
                    output.push(indexedItem);
                else
                    submit(() -> output.push(indexedItem));
            }
        });
    }

    @Override
    void internalClose() {
        Stream.of(outputs).forEach(Pipe::setEndOfInput);
    }
}
