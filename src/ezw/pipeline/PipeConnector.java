package ezw.pipeline;

/**
 * An internal pipeline worker that only moves items between pipes.
 */
abstract class PipeConnector extends PipelineWorker {

    PipeConnector(int parallel) {
        super(parallel);
    }

    @Override
    protected int getParallel() {
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
