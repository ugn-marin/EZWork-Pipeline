package ezw.pipeline;

import ezw.concurrent.*;
import ezw.util.Lazy;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A callable runnable executing in a pipeline.
 */
public abstract class PipelineWorker implements CallableRunnable {
    private final int parallel;
    private final Lazy<ExecutorService> executorService;
    private final Lazy<CancellableSubmitter> cancellableSubmitter;
    private final AtomicBoolean executed = new AtomicBoolean();
    private final AtomicInteger cancelledWork = new AtomicInteger();
    private Throwable throwable;

    PipelineWorker(int parallel) {
        this.parallel = parallel;
        executorService = new Lazy<>(() -> new BlockingThreadPoolExecutor(parallel));
        cancellableSubmitter = new Lazy<>(() -> new CancellableSubmitter(executorService.get()));
    }

    /**
     * Returns the parallel level.
     */
    protected int getParallel() {
        return parallel;
    }

    /**
     * Executes the worker synchronously until all internal work is done, or an exception is thrown.
     * @throws Exception An exception terminating the pipeline. May come from a worker, or the cancel argument.
     */
    @Override
    public void run() throws Exception {
        if (executed.getAndSet(true))
            throw new UnsupportedOperationException("The pipeline worker instance cannot be reused.");
        try {
            work();
            join();
        } catch (Throwable t) {
            setThrowable(t);
        } finally {
            onFinish(throwable instanceof SilentStop ? null : throwable);
        }
    }

    /**
     * Submits internal work as a cancellable task. Blocked if parallel level reached. The work execution failure will
     * trigger cancellation of all submitted work and failure of the entire worker.
     * @param work Internal work.
     * @throws InterruptedRuntimeException If interrupted while trying to submit the work.
     */
    void submit(CallableRunnable work) throws InterruptedRuntimeException {
        cancellableSubmitter.get().submit(() -> {
            try {
                return work.toCallable().call();
            } catch (Throwable t) {
                cancel(t);
                throw t;
            }
        });
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
     * will not throw an exception as a result of this operation. Equivalent to:<br><code><pre>
     * cancel(null);</pre></code>
     */
    public void stop() {
        cancel(null);
    }

    /**
     * Returns the total number of tasks that failed, were cancelled after submitting, or interrupted. The full count is
     * only reached after the execution returns or throws an exception.
     */
    public int getCancelledWork() {
        return cancelledWork.get();
    }

    /**
     * Waits for all submitted tasks by shutting the thread pool down and awaiting termination.
     * @throws InterruptedException If interrupted.
     */
    protected void join() throws InterruptedException {
        if (executorService.isCalculated())
            Concurrent.join(executorService.get());
    }

    /**
     * Submits all internal work.
     * @throws InterruptedException If interrupted.
     */
    protected abstract void work() throws InterruptedException;

    /**
     * Runs after all internal work is done.
     * @param throwable The throwable thrown by the work, or any submitted work. Null if finished successfully, or if
     *                  stopped by calling <code>stop</code> or <code>cancel(null)</code>.
     * @throws Exception The throwable if not null, thrown as is if instance of Exception or Error, wrapped in a new
     * UndeclaredThrowableException otherwise.
     */
    protected void onFinish(Throwable throwable) throws Exception {
        if (throwable == null)
            return;
        if (throwable instanceof Error)
            throw (Error) throwable;
        else if (throwable instanceof Exception)
            throw (Exception) throwable;
        throw new UndeclaredThrowableException(throwable);
    }

    @Override
    public String toString() {
        Class<?> clazz = getClass();
        String simpleName = clazz.getSimpleName();
        while (simpleName.isEmpty()) {
            clazz = clazz.getSuperclass();
            simpleName = clazz.getSimpleName();
        }
        if (getParallel() > 1)
            simpleName += String.format("[%d]", getParallel());
        return simpleName;
    }

    private static class SilentStop extends Throwable {}
}
