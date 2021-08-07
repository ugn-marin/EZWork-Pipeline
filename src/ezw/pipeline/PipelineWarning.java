package ezw.pipeline;

/**
 * Represents a potential problem with the pipeline structure, that makes it non-standard and possibly faulty.
 */
public enum PipelineWarning {
    /**
     * Indicates that the pipeline couldn't discover all the attached workers by following the pipes of the input and
     * output workers, forks and joins. This might mean that some workers are not properly interconnected by pipes, or
     * don't properly implement the input/output worker interfaces. If intentional, make sure the workers have the means
     * to get/send their input/output.
     */
    DISCOVERY("Not all workers are discoverable."),
    /**
     * Indicates that one or more pipes is used as an output for more than one worker. For instance, several suppliers.
     * This structure is supported, but has the danger of failing one of the workers on <i>Attempting to push into pipe
     * after end of input</i>. Make sure to override the workers' <code>join</code> in a way that handles that, or that
     * the input never ends in a race condition between the relevant workers.
     */
    MULTIPLE_INPUTS("Multiple workers push into the same pipe."),
    /**
     * Indicates that a pipe is used in different levels of the flow, creating a potential cycle. This will probably
     * make the pipeline not work properly.
     */
    CYCLE("Cycle detected."),
    /**
     * A pipe was extended without using the <code>extend</code> method in the pipeline builder. The pipeline may still
     * be functional, but not all pipes are depicted by the pipeline chart.
     */
    EXTENSION("Not all extended pipes are mapped.");

    private final String description;

    PipelineWarning(String description) {
        this.description = description;
    }

    /**
     * Returns the warning description.
     */
    public String getDescription() {
        return description;
    }
}
