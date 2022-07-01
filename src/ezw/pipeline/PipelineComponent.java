package ezw.pipeline;

/**
 * A component of a pipeline: Pipe or worker.
 */
public interface PipelineComponent {

    /**
     * Returns the name of the component.
     */
    String getName();
}
