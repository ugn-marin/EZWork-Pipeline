package ezw.pipeline;

/**
 * A pipe consumer draining a pipe with no additional logic.
 * @param <I> The input items type.
 */
final class PipeDrain<I> extends PipeConsumer<I> {

    PipeDrain(Pipe<I> input) {
        super(true, input, 0);
    }

    @Override
    protected void work() {
        getInput().drain();
    }

    @Override
    public void accept(I item) {}

    @Override
    protected String getName() {
        return "x";
    }
}
