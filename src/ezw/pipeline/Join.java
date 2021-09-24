package ezw.pipeline;

import ezw.Sugar;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipe connector joining input items from several pipes into one output pipe. Join is a barrier for each index,
 * meaning that an item is only pushed once it was received from all input pipes. For that reason, all input pipes must
 * be <b>in the same index scope</b>.<br>
 * If any of the input items with a given index is marked modified, the first modified item is pushed, else the last
 * received item is pushed.
 * @param <I> The items type.
 */
final class Join<I> extends PipeConnector implements OutputWorker<I> {
    private final Pipe<I>[] inputs;
    private final Pipe<I> output;
    private final Map<Long, Integer> remainingInputs;
    private final Map<Long, IndexedItem<I>> modifiedInputs;

    @SafeVarargs
    Join(Pipe<I> output, Pipe<I>... inputs) {
        super(Sugar.requireNoneNull(inputs).length);
        if (inputs.length < 2)
            throw new PipelineConfigurationException("Join requires at least 2 input pipes.");
        if (!Sugar.instancesOf(List.of(inputs), SupplyGate.class).isEmpty())
            throw new PipelineConfigurationException("Joining different index scopes.");
        this.inputs = inputs;
        this.output = Objects.requireNonNull(output, "Output pipe is required.");
        remainingInputs = new HashMap<>(inputs.length);
        modifiedInputs = new HashMap<>(inputs.length);
    }

    Pipe<I>[] getInputs() {
        return inputs;
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

    @Override
    void internalClose() {
        output.setEndOfInput();
    }
}
