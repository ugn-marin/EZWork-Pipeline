package ezw.pipeline;

/**
 * Represents a potential problem with the pipeline structure, that makes it non-standard and possibly faulty.
 */
public enum PipelineWarning {
    /**
     * Indicates that the pipeline couldn't discover all of the attached workers by following the pipes of the input and
     * output components, forks and joins. This might mean that some workers are not properly interconnected by pipes,
     * or don't properly implement the input/output component interfaces. If intentional, make sure the workers have
     * other means to get/send their input/output.
     */
    DISCOVERY("Not all workers are discoverable"),
    /**
     * Indicates that one or more pipes is used as an output for more than one worker. For instance, several suppliers.
     * This structure is supported, but has the danger of failing one of the components on <i>Attempting to push into
     * pipe after end of input</i>. Make sure to override the workers' <code>join</code> in a way that handles that.
     */
    MULTIPLE_INPUTS("Multiple components push into the same pipe"),
    /**
     * Indicates that a pipe is used in different levels of the flow, creating a potential cycle. This will probably
     * make the pipeline not work properly.
     */
    CYCLE("Cycle detected");

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
