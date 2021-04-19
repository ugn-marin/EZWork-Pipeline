package ezw.pipeline;

import ezw.concurrent.InterruptedRuntimeException;
import ezw.concurrent.Interruptible;

import java.util.HashMap;
import java.util.Iterator;
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
public class Pipe<I> implements Iterable<IndexedItem<I>> {
    private static final long POLLING_TIMEOUT = 100;

    protected final int baseCapacity;
    private final PipeIterator iterator = new PipeIterator();
    private final ReentrantLock lock = new ReentrantLock(true);
    private final BlockingQueue<IndexedItem<I>> inOrderQueue;
    private final Map<Long, IndexedItem<I>> outOfOrderItems;
    private final AtomicInteger inPush = new AtomicInteger();
    private long expectedIndex = 0;
    private boolean endOfInput = false;

    /**
     * Creates a pipe.
     * @param baseCapacity The base capacity (<code>BC</code>) of the pipe. Used as the capacity for the in-order queue,
     *                     as well as the out-of-order items cache. Together with the in-push items, which depends on
     *                     the number of the pushing threads <code>N</code>, the total maximum theoretical capacity of
     *                     the pipe can reach <code>BC+N</code>.
     */
    public Pipe(int baseCapacity) {
        this.baseCapacity = baseCapacity;
        inOrderQueue = new ArrayBlockingQueue<>(baseCapacity, true);
        outOfOrderItems = new HashMap<>();
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
            throw new IllegalStateException("Attempted to push into pipe after end of input.");
        while (true) {
            lock.lock();
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
        if (indexedItem.getIndex() == expectedIndex) {
            inOrderQueue.put(indexedItem);
            expectedIndex++;
        } else {
            if (outOfOrderItems.size() > inPushItems())
                return false;
            outOfOrderItems.put(indexedItem.getIndex(), indexedItem);
        }
        pushItem(outOfOrderItems.remove(expectedIndex));
        synchronized (lock) {
            lock.notifyAll();
        }
        return true;
    }

    private IndexedItem<I> poll() throws InterruptedException {
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

    @Override
    public Iterator<IndexedItem<I>> iterator() {
        return iterator;
    }

    @Override
    public String toString() {
        return String.format("-<%d>-", baseCapacity);
    }

    private class PipeIterator implements Iterator<IndexedItem<I>> {
        private IndexedItem<I> next;

        @Override
        public boolean hasNext() throws InterruptedRuntimeException {
            return (next = Interruptible.call(Pipe.this::poll)) != null;
        }

        @Override
        public IndexedItem<I> next() {
            return next;
        }
    }
}
