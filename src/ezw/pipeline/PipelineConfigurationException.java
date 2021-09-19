package ezw.pipeline;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * An exception thrown by the pipeline building logic, indicating an illegal pipeline build.
 */
public class PipelineConfigurationException extends RuntimeException {
    private Set<PipelineWarning> pipelineWarnings;

    PipelineConfigurationException(String message) {
        super(message);
    }

    PipelineConfigurationException(Set<PipelineWarning> pipelineWarnings) {
        this(describeWarnings(pipelineWarnings));
        this.pipelineWarnings = pipelineWarnings;
    }

    private static String describeWarnings(Set<PipelineWarning> pipelineWarnings) {
        String s = pipelineWarnings.size() > 1 ? "s" : "";
        String and = pipelineWarnings.size() > 1 ? "and/" : "";
        String n = pipelineWarnings.size() > 1 ? System.lineSeparator() : " ";
        return String.format("Got pipeline warning%s:%s%s%sFix the building logic %sor pass warning%s as allowed.", s,
                n, pipelineWarnings.stream().map(PipelineWarning::getDescription).collect(Collectors.joining(n)), n,
                and, s);
    }

    /**
     * Returns the warnings that caused this exception, if any.
     */
    public Set<PipelineWarning> getWarnings() {
        return pipelineWarnings;
    }
}
