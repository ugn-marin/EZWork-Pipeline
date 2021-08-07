package ezw.pipeline;

/**
 * A pipe connector marking a pipe as extended. The extender does not interact with the pipe.
 * @param <I> The items type.
 */
final class Extender<I> extends PipeConnector implements InputWorker<I>, OutputWorker<I> {
    private final Pipe<I> pipe;

    Extender(Pipe<I> pipe) {
        super(0);
        this.pipe = pipe;
    }

    @Override
    protected void work() {}

    @Override
    public Pipe<I> getInput() {
        return pipe;
    }

    @Override
    public Pipe<I> getOutput() {
        return pipe;
    }

    @Override
    public String toString() {
        return "-";
    }
}
