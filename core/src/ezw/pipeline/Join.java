package ezw.pipeline;

import ezw.concurrent.Interruptible;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline worker joining input items from several pipes into one output pipe. All input pipes must be in the same
 * index scope. An item is pushed once it was received from all input pipes. If any of the input items with a given
 * index is marked modified, the first modified item is pushed, else the last received item is pushed.
 * @param <I> The items type.
 */
final class Join<I> extends PipelineWorker implements OutputComponent<I> {
    private final Pipe<I>[] inputs;
    private final Pipe<I> output;
    private final Map<Long, Integer> remainingInputs;
    private final Map<Long, IndexedItem<I>> modifiedInputs;

    @SafeVarargs
    Join(Pipe<I> output, Pipe<I>... inputs) {
        super(inputs.length);
        Arrays.stream(inputs).forEach(Objects::requireNonNull);
        if (inputs.length < 2)
            throw new IllegalArgumentException("Join requires at least 2 input pipes.");
        this.inputs = inputs;
        this.output = Objects.requireNonNull(output);
        remainingInputs = new HashMap<>(inputs.length);
        modifiedInputs = new HashMap<>(inputs.length);
    }

    @Override
    public Pipe<I> getOutput() {
        return output;
    }

    @Override
    protected void work() {
        for (Pipe<I> input : inputs) {
            submit(() -> {
                for (IndexedItem<I> indexedItem : input) {
                    push(indexedItem);
                }
            });
        }
    }

    @Override
    void join() throws InterruptedException {
        super.join();
        output.setEndOfInput();
    }

    private void push(IndexedItem<I> indexedItem) throws InterruptedException {
        long index = indexedItem.getIndex();
        boolean push = false;
        IndexedItem<I> modified = null;
        synchronized (remainingInputs) {
            if (indexedItem.isModified() && !modifiedInputs.containsKey(index))
                modifiedInputs.put(index, indexedItem);
            if (!remainingInputs.containsKey(index)) {
                remainingInputs.put(index, inputs.length - 1);
            } else {
                int remaining = remainingInputs.get(index);
                if (remaining == 1) {
                    remainingInputs.remove(index);
                    remainingInputs.notifyAll();
                    push = true;
                    modified = modifiedInputs.remove(index);
                } else {
                    remainingInputs.put(index, remaining - 1);
                }
            }
            if (!push) {
                while (remainingInputs.containsKey(index)) {
                    remainingInputs.wait();
                }
                return;
            }
        }
        output.push(modified != null ? modified : indexedItem);
    }
}
