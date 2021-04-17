package ezw.pipeline;

import ezw.concurrent.BlockingThreadPoolExecutor;
import ezw.concurrent.CallableRunnable;
import ezw.concurrent.CancellableSubmitter;
import ezw.concurrent.InterruptedRuntimeException;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A callable runnable executing in a pipeline.
 */
public abstract class PipelineWorker implements CallableRunnable {
    private final BlockingThreadPoolExecutor blockingThreadPoolExecutor;
    private final CancellableSubmitter cancellableSubmitter;
    private final AtomicBoolean executed = new AtomicBoolean();
    private Throwable throwable;

    PipelineWorker(int parallel) {
        blockingThreadPoolExecutor = new BlockingThreadPoolExecutor(parallel);
        cancellableSubmitter = new CancellableSubmitter(blockingThreadPoolExecutor);
    }

    /**
     * Returns the parallel level.
     */
    protected int getParallel() {
        return blockingThreadPoolExecutor.getMaximumPoolSize();
    }

    /**
     * Executes the worker until all internal work is done, or an exception thrown.
     * @throws Exception An exception terminating the pipeline.
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
        cancellableSubmitter.submit(() -> {
            try {
                return work.toCallable().call();
            } catch (Throwable t) {
                cancel(t);
                throw t;
            }
        });
    }

    private void setThrowable(Throwable throwable) {
        synchronized (blockingThreadPoolExecutor) {
            if (this.throwable == null)
                this.throwable = throwable;
        }
    }

    /**
     * Cancels the execution of all internal work, interrupts if possible.
     * @param t The throwable for the worker to throw. Not allowed to be null - use <code>stop</code> to stop the worker
     *          without an exception.
     */
    public void cancel(Throwable t) {
        setThrowable(Objects.requireNonNull(t, "Throwable is required."));
        blockingThreadPoolExecutor.shutdown();
        cancellableSubmitter.cancelSubmitted();
    }

    /**
     * Cancels the execution of all internal work, interrupts if possible. The worker will not throw an exception.
     */
    public void stop() {
        cancel(new SilentStop());
    }

    /**
     * Waits for all submitted tasks by shutting the thread pool down and awaiting termination.
     * @throws InterruptedException If interrupted.
     */
    void join() throws InterruptedException {
        blockingThreadPoolExecutor.join();
    }

    /**
     * Submits all internal work.
     * @throws InterruptedException If interrupted.
     */
    protected abstract void work() throws InterruptedException;

    /**
     * Runs after all internal work is done.
     * @param throwable The throwable thrown by the work, or any submitted work. Null if finished successfully, or if
     *                  stopped by calling <code>stop</code>.
     * @throws Exception The throwable if not null.
     */
    protected void onFinish(Throwable throwable) throws Exception {
        if (throwable == null)
            return;
        if (throwable instanceof Error)
            throw (Error) throwable;
        else if (throwable instanceof Exception)
            throw (Exception) throwable;
        throw new Exception(throwable);
    }

    private static class SilentStop extends Throwable {}
}
