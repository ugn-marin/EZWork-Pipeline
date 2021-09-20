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
        boolean multiple = pipelineWarnings.size() > 1;
        String a = multiple ? "" : " a";
        String s = multiple ? "s" : "";
        String and = multiple ? "and/" : "";
        String n = multiple ? System.lineSeparator() : " ";
        String l = multiple ? n + "- " : n;
        String these = multiple ? "these" : "this";
        return String.format("Got%s pipeline warning%s:%s%s%sFix the building logic %sor pass %s warning%s as allowed.",
                a, s, l, pipelineWarnings.stream().map(PipelineWarning::getDescription).collect(Collectors.joining(l)),
                n, and, these, s);
    }

    /**
     * Returns the warnings that caused this exception, if any.
     */
    public Set<PipelineWarning> getWarnings() {
        return pipelineWarnings;
    }
}
