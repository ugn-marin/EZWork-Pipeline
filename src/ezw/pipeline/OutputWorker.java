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
}
