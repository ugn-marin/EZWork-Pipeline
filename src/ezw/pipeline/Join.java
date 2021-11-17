package ezw.pipeline;

import ezw.Sugar;
import ezw.function.Reducer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * An internal worker joining input items from several pipes into one output pipe. Join is a barrier for each index,
 * meaning that an item is only pushed once it was received from all input pipes. For that reason, all input pipes must
 * be <b>in the same index scope</b>.<br>
 * The item pushed into the output pipe for every index is computed by the reducer provided, or if wasn't provided, the
 * last item for every index is pushed.
 * @param <I> The items type.
 */
final class Join<I> extends PipelineWorker implements OutputWorker<I> {
    private final Pipe<I>[] inputs;
    private final Pipe<I> output;
    private final Reducer<I> reducer;
    private final Map<Long, List<IndexedItem<I>>> allInputs;
    private final Map<Long, Integer> remainingInputs;

    @SafeVarargs
    Join(Reducer<I> reducer, Pipe<I> output, Pipe<I>... inputs) {
        super(true, Sugar.requireNoneNull(inputs).length);
        if (inputs.length < 2)
            throw new PipelineConfigurationException("Join requires at least 2 input pipes.");
        if (!Sugar.instancesOf(List.of(inputs), SupplyGate.class).isEmpty())
            throw new PipelineConfigurationException("Joining different index scopes.");
        this.inputs = inputs;
        this.output = Objects.requireNonNull(output, "Output pipe is required.");
        this.reducer = Objects.requireNonNullElse(reducer, Sugar::last);
        allInputs = new HashMap<>(inputs.length);
        remainingInputs = new HashMap<>(inputs.length);
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
            allInputs.computeIfAbsent(indexedItem.getIndex(), i -> new ArrayList<>()).add(indexedItem);
            if (!remainingInputs.containsKey(index)) {
                remainingInputs.put(index, inputs.length - 1);
            } else {
                int remaining = remainingInputs.get(index);
                push = remaining == 1;
                if (push)
                    next = getNext(index);
                else
                    remainingInputs.put(index, remaining - 1);
            }
            if (!push) {
                while (remainingInputs.containsKey(index)) {
                    remainingInputs.wait();
                }
                return;
            }
        }
        output.push(next);
    }

    private IndexedItem<I> getNext(long index) {
        remainingInputs.remove(index);
        remainingInputs.notifyAll();
        return new IndexedItem<>(index, reducer.apply(allInputs.remove(index).stream().map(IndexedItem::getItem)
                .collect(Collectors.toList())));
    }

    @Override
    void internalClose() {
        output.setEndOfInput();
        synchronized (remainingInputs) {
            allInputs.clear();
            remainingInputs.clear();
            remainingInputs.notifyAll();
        }
    }
}
