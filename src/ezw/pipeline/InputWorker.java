package ezw.pipeline;

/**
 * A worker containing an input pipe.
 * @param <I> The input items type.
 */
public interface InputWorker<I> {

    /**
     * Return the input pipe.
     */
    Pipe<I> getInput();
}
