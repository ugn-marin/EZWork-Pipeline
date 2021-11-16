package ezw.pipeline;

/**
 * A worker containing an output pipe.
 * @param <O> The output items type.
 */
public interface OutputWorker<O> {

    /**
     * Returns the output pipe.
     */
    Pipe<O> getOutput();

    /**
     * Constructs a pipe consumer draining the output pipe of this worker.
     */
    default PipeConsumer<O> drain() {
        return new PipeDrain<>(getOutput());
    }
}
