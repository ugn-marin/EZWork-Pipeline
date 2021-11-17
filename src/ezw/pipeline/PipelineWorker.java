package ezw.pipeline;

import ezw.Sugar;
import ezw.concurrent.*;
import ezw.flow.OneShot;
import ezw.function.UnsafeRunnable;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An unsafe runnable executing in a pipeline.
 */
public abstract class PipelineWorker implements UnsafeRunnable {
    private static final AtomicInteger workerThreadNumber = new AtomicInteger();

    private final boolean internal;
    private final int concurrency;
    private final Lazy<ExecutorService> executorService;
    private final Lazy<CancellableSubmitter> cancellableSubmitter;
    private final OneShot oneShot = new OneShot();
    private final Latch latch = new Latch();
    private final AtomicInteger cancelledWork = new AtomicInteger();
    private Throwable throwable;

    PipelineWorker(boolean internal, int concurrency) {
        this.internal = internal;
        this.concurrency = concurrency;
        executorService = new Lazy<>(() -> new BlockingThreadPoolExecutor(concurrency, Concurrent.namedThreadFactory(
                String.format("PW %d (%s)", workerThreadNumber.incrementAndGet(), getName()))));
        cancellableSubmitter = new Lazy<>(() -> new CancellableSubmitter(executorService.get()));
    }

    boolean isInternal() {
        return internal;
    }

    /**
     * Returns the concurrency level of the worker.
     */
    protected int getConcurrency() {
        return internal ? 0 : concurrency;
    }

    /**
     * Executes the worker synchronously until all internal work is done, or an exception is thrown.
     * @throws Exception An exception terminating the pipeline. May come from a worker, or the cancel argument.
     */
    @Override
    public void run() throws Exception {
        oneShot.check("The pipeline worker instance cannot be reused.");
        interceptThrowable(() -> {
            work();
            executorService.maybe(pool -> Interruptible.run(() -> Concurrent.join(pool)));
        }, () -> interceptThrowable(this::close, () -> interceptThrowable(this::internalClose, () -> {
            latch.release();
            Sugar.throwIfNonNull(throwable instanceof SilentStop ? null : throwable);
        })));
    }

    private void interceptThrowable(UnsafeRunnable run, UnsafeRunnable close) throws Exception {
        try {
            run.run();
        } catch (Throwable t) {
            setThrowable(t);
        } finally {
            close.run();
        }
    }

    /**
     * Causes the current thread to wait until all internal work is done, or an exception is thrown. Returns normally
     * regardless of the result.
     * @throws InterruptedException If current thread was interrupted.
     */
    public void await() throws InterruptedException {
        latch.await();
    }

    /**
     * Submits internal work as a cancellable task. Blocked if concurrency level reached. The work execution failure
     * will trigger cancellation of all submitted work and failure of the entire worker.
     * @param work Internal work.
     * @throws InterruptedRuntimeException If interrupted while trying to submit the work.
     */
    void submit(UnsafeRunnable work) throws InterruptedRuntimeException {
        cancellableSubmitter.get().submit(() -> {
            Sugar.throwIfNonNull(throwable);
            try {
                return work.toVoidCallable().call();
            } catch (Throwable t) {
                cancel(t);
                throw t;
            }
        });
    }

    Throwable getThrowable() {
        return throwable;
    }

    private void setThrowable(Throwable throwable) {
        synchronized (executorService) {
            if (this.throwable == null)
                this.throwable = Objects.requireNonNullElse(throwable, new SilentStop());
        }
    }

    /**
     * Cancels the execution of all internal work, interrupts if possible. Does not wait for work to stop.
     * @param throwable The throwable for the worker to throw. If null, nothing will be thrown upon stoppage.
     */
    public void cancel(Throwable throwable) {
        setThrowable(throwable);
        if (executorService.isCalculated()) {
            executorService.get().shutdown();
            if (cancellableSubmitter.isCalculated())
                cancelledWork.addAndGet(cancellableSubmitter.get().cancelSubmitted());
        }
    }

    /**
     * Cancels the execution of all internal work, interrupts if possible. Does not wait for work to stop. The worker
     * will throw an InterruptedException.
     */
    public void interrupt() {
        cancel(new InterruptedException("Controlled interruption."));
    }

    /**
     * Cancels the execution of all internal work, interrupts if possible. Does not wait for work to stop. The worker
     * will not throw an exception as a result of this operation. Equivalent to:
     * <pre>
     * cancel(null);
     * </pre>
     */
    public void stop() {
        cancel(null);
    }

    /**
     * Returns the total number of tasks that failed, were cancelled after submitting, or interrupted. The full count is
     * only reached after the execution returns or throws an exception.
     */
    public int getCancelledWork() {
        return internal ? 0 : cancelledWork.get();
    }

    /**
     * Submits all internal work.
     * @throws InterruptedException If interrupted.
     */
    protected abstract void work() throws InterruptedException;

    /**
     * Called automatically when the worker is done executing or failed.
     * @throws Exception A possible exception from the closing logic. Will be thrown by the pipeline if and only if it
     * isn't already in the process of throwing a different exception.
     */
    protected void close() throws Exception {}

    void internalClose() {}

    /**
     * Returns the name of the worker.
     */
    protected String getName() {
        Class<?> clazz = getClass();
        String simpleName = clazz.getSimpleName();
        while (simpleName.isEmpty()) {
            clazz = clazz.getSuperclass();
            simpleName = clazz.getSimpleName();
        }
        if (simpleName.length() > 5 && clazz.getPackage().equals(PipelineWorker.class.getPackage()))
            simpleName = String.valueOf(simpleName.toCharArray()[4]);
        else if (internal)
            simpleName = simpleName.toLowerCase();
        return simpleName;
    }

    @Override
    public String toString() {
        String string = getName();
        if (!internal && getConcurrency() != 1)
            string += String.format("[%d]", getConcurrency());
        return string;
    }

    private static class SilentStop extends Throwable {}
}
