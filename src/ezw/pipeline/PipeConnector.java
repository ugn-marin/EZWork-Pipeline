package ezw.pipeline;

/**
 * An internal pipeline worker that only moves items between pipes.
 */
abstract class PipeConnector extends PipelineWorker {

    PipeConnector(int concurrency) {
        super(concurrency);
    }

    @Override
    protected int getConcurrency() {
        return 0;
    }

    @Override
    public int getCancelledWork() {
        return 0;
    }

    @Override
    protected String getName() {
        return super.getName().toLowerCase();
    }

    @Override
    public String toString() {
        return getName();
    }
}
