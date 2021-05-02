package ezw.pipeline;

/**
 * A component containing an output pipe.
 * @param <O> The output items type.
 */
public interface OutputComponent<O> {

    /**
     * Returns the output pipe.
     */
    Pipe<O> getOutput();
}
