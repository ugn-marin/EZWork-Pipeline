package ezw.pipeline;

import ezw.Sugar;
import ezw.function.Reducer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A pipe connector joining input items from several pipes into one output pipe. Join is a barrier for each index,
 * meaning that an item is only pushed once it was received from all input pipes. For that reason, all input pipes must
 * be <b>in the same index scope</b>.<br>
 * The item pushed into the output pipe is reduced by the following logic:<br>
 * 1. If a reducer was provided, the item is reduced from all items received for the particular index.<br>
 * 2. If a reducer was not provided, and any of the input items with a given index is marked modified, the last modified
 * item is pushed.<br>
 * 3. For other cases (no reducer provided or no items modified), the last item received for the particular index is
 * pushed.
 * @param <I> The items type.
 */
final class Join<I> extends PipeConnector implements OutputWorker<I> {
    private final Pipe<I>[] inputs;
    private final Pipe<I> output;
    private final Reducer<I> reducer;
    private final Map<Long, Integer> remainingInputs;
    private Map<Long, IndexedItem<I>> modifiedInputs;
    private Map<Long, List<IndexedItem<I>>> allInputs;

    @SafeVarargs
    Join(Reducer<I> reducer, Pipe<I> output, Pipe<I>... inputs) {
        super(Sugar.requireNoneNull(inputs).length);
        if (inputs.length < 2)
            throw new PipelineConfigurationException("Join requires at least 2 input pipes.");
        if (!Sugar.instancesOf(List.of(inputs), SupplyGate.class).isEmpty())
            throw new PipelineConfigurationException("Joining different index scopes.");
        this.inputs = inputs;
        this.output = Objects.requireNonNull(output, "Output pipe is required.");
        this.reducer = reducer;
        remainingInputs = new HashMap<>(inputs.length);
        if (reducer == null)
            modifiedInputs = new HashMap<>(inputs.length);
        else
            allInputs = new HashMap<>(inputs.length);
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
        for (var input : inputs) {
            submit(() -> {
                for (var indexedItem : input) {
                    push(indexedItem);
                }
            });
        }
    }

    private void push(IndexedItem<I> indexedItem) throws InterruptedException {
        long index = indexedItem.getIndex();
        boolean push = false;
        IndexedItem<I> next = null;
        synchronized (remainingInputs) {
            mapForReduce(indexedItem);
            if (!remainingInputs.containsKey(index)) {
                remainingInputs.put(index, inputs.length - 1);
            } else {
                int remaining = remainingInputs.get(index);
                if (remaining == 1) {
                    remainingInputs.remove(index);
                    remainingInputs.notifyAll();
                    push = true;
                    next = reduce(index);
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
        output.push(next != null ? next : indexedItem);
    }

    private void mapForReduce(IndexedItem<I> indexedItem) {
        if (reducer == null) {
            if (indexedItem.isModified())
                modifiedInputs.put(indexedItem.getIndex(), indexedItem);
        } else {
            allInputs.computeIfAbsent(indexedItem.getIndex(), i -> new ArrayList<>()).add(indexedItem);
        }
    }

    private IndexedItem<I> reduce(long index) {
        if (reducer == null)
            return modifiedInputs.remove(index);
        return new IndexedItem<>(index, reducer.apply(allInputs.remove(index).stream().map(IndexedItem::getItem)
                .collect(Collectors.toList())), true);
    }

    @Override
    void internalClose() {
        output.setEndOfInput();
    }
}
