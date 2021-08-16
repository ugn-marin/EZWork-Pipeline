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
    public String toString() {
        return super.toString().toLowerCase();
    }
}
