package ezw.pipeline;

import ezw.Sugar;
import ezw.calc.Scale;
import ezw.concurrent.Interruptible;
import ezw.function.UnsafeConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A queue of items moved between pipeline workers.
 * @param <I> The items type.
 */
public abstract class Pipe<I> {
    private static final long POLLING_TIMEOUT = 100;

    private final int baseCapacity;
    private final String name;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final BlockingQueue<IndexedItem<I>> inOrderQueue;
    private final Map<Long, IndexedItem<I>> outOfOrderItems;
    private final AtomicInteger inPush = new AtomicInteger();
    private long expectedIndex = 0;
    private long totalsSum = 0;
    private boolean endOfInput = false;

    /**
     * Constructs a pipe.
     * @param baseCapacity The base capacity (<code>BC</code>) of the pipe. Used as the capacity for the in-order queue,
     *                     as well as the out-of-order items cache. Together with the in-push items, which depends on
     *                     the number of the pushing threads <code>N</code>, the total maximum theoretical capacity of
     *                     the pipe can reach <code>BC+N</code>.
     * @param name The name of the pipe.
     */
    protected Pipe(int baseCapacity, String name) {
        this.baseCapacity = Sugar.requireRange(baseCapacity, 1, null);
        this.name = name;
        inOrderQueue = new ArrayBlockingQueue<>(baseCapacity, true);
        outOfOrderItems = new HashMap<>();
    }

    /**
     * Returns the base capacity.
     */
    protected int getBaseCapacity() {
        return baseCapacity;
    }

    /**
     * Returns the estimated number of items currently in this pipe, which is the sum of in-order items, out-of-order
     * items and in-push items.
     */
    public int totalItems() {
        return inOrderItems() + outOfOrderItems() + inPushItems();
    }

    /**
     * Returns the number of items currently in queue.
     */
    public int inOrderItems() {
        return inOrderQueue.size();
    }

    /**
     * Returns the number of items waiting to be arranged in the queue.
     */
    public int outOfOrderItems() {
        return outOfOrderItems.size();
    }

    /**
     * Returns the number of items currently in <code>push</code>.
     */
    public int inPushItems() {
        return inPush.get();
    }

    /**
     * Returns the number of items pushed into the queue so far (that passed the in-push and arrangement phases).
     */
    public long getItemsPushed() {
        return expectedIndex;
    }

    /**
     * Returns the average load of the pipe up to this point (average size out of base capacity), between 0 and 1.
     */
    public double getAverageLoad() {
        return Scale.getDefault().apply(expectedIndex == 0 ? 0 :
                Math.min((double) totalsSum / (expectedIndex * baseCapacity), 1));
    }

    void push(IndexedItem<I> indexedItem) throws InterruptedException {
        inPush.incrementAndGet();
        try {
            pushItem(indexedItem);
        } finally {
            inPush.decrementAndGet();
        }
    }

    private void pushItem(IndexedItem<I> indexedItem) throws InterruptedException {
        if (indexedItem == null)
            return;
        if (endOfInput)
            throw new IllegalStateException("Attempting to push into pipe after end of input.");
        while (true) {
            lock.lockInterruptibly();
            try {
                if (tryPush(indexedItem))
                    break;
            } finally {
                lock.unlock();
            }
            synchronized (lock) {
                lock.wait(POLLING_TIMEOUT);
            }
        }
    }

    private boolean tryPush(IndexedItem<I> indexedItem) throws InterruptedException {
        if (indexedItem.index() == expectedIndex) {
            inOrderQueue.put(indexedItem);
            expectedIndex++;
            totalsSum += totalItems() - 1;
        } else {
            if (outOfOrderItems.size() > inPushItems())
                return false;
            outOfOrderItems.put(indexedItem.index(), indexedItem);
        }
        pushItem(outOfOrderItems.remove(expectedIndex));
        synchronized (lock) {
            lock.notifyAll();
        }
        return true;
    }

    private IndexedItem<I> take() throws InterruptedException {
        while (!endOfInput) {
            IndexedItem<I> indexedItem = inOrderQueue.poll(POLLING_TIMEOUT, TimeUnit.MILLISECONDS);
            if (indexedItem != null)
                return indexedItem;
        }
        return inOrderQueue.poll();
    }

    /**
     * Sets the pipe end-of-input flag, indicating there's no more items to poll from it. Pushing items after setting
     * the flag will result in an exception.
     */
    public void setEndOfInput() {
        endOfInput = true;
    }

    void drain() throws InterruptedException {
        drain(i -> {});
    }

    void drain(UnsafeConsumer<IndexedItem<I>> action) throws InterruptedException {
        var consumer = action.toConsumer();
        IndexedItem<I> next;
        while ((next = take()) != null)
            consumer.accept(next);
    }

    void clear() {
        for (int i = 0; totalItems() > 0; i++) {
            final int waitTime = i * 10 + 1;
            synchronized (lock) {
                Interruptible.run(() -> lock.wait(waitTime));
            }
            inOrderQueue.clear();
            outOfOrderItems.clear();
        }
    }

    @Override
    public String toString() {
        return String.format("-<%s:%d>-", name, baseCapacity);
    }

    /**
     * An item wrapping containing its assigned index in the scope.
     * @param <I> The item type.
     */
    record IndexedItem<I>(long index, I item) {}
}
