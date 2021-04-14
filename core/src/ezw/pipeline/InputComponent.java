package ezw.pipeline;

/**
 * A component containing an input pipe.
 * @param <I> The input items type.
 */
public interface InputComponent<I> {

    /**
     * Return the input pipe.
     */
    Pipe<I> getInput();
}
