package ezw.pipeline;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Analyzer {
    private final Pipeline<?> pipeline;
    private final Map<Object, Integer> levels;
    private final Map<Integer, List<Object>> levelsAggregation;
    private final Predicate<Object> unleveled;

    private Set<OutputComponent<?>> outputComponents;
    private Set<InputComponent<?>> inputComponents;
    private Set<Fork<?>> forks;
    private Set<Join<?>> joins;

    public Analyzer(Pipeline<?> pipeline) {
        if (pipeline.getPipelineWorkers().stream().anyMatch(pw ->
                !(pw instanceof OutputComponent || pw instanceof InputComponent)))
            throw new IllegalArgumentException("...");
        this.pipeline = pipeline;
        levels = new HashMap<>(pipeline.getPipelineWorkers().size());
        levelsAggregation = new HashMap<>(2);
        unleveled = Predicate.not(levels::containsKey);
    }

    void analyze() {
        classifyPipelineWorkers();
        Set<Object> connectionsLeveled = new HashSet<>(pipeline.getPipelineWorkers().size());
        setLevel(pipeline.getPipelineWorkers().stream().findFirst().orElseThrow(), 0);
        while (levels.size() < pipeline.getPipelineWorkers().size()) {
            Map.copyOf(levels).entrySet().stream().filter(entry -> !connectionsLeveled.contains(entry.getKey()))
                    .forEach(entry -> {
                var pipelineWorker = entry.getKey();
                var level = entry.getValue();
                setLevel(pipelineWorker, level);
                setConnectionsLevel(pipelineWorker, level);
                connectionsLeveled.add(pipelineWorker);
            });
        }
        for (int i = levels.values().stream().min(Integer::compareTo).orElseThrow();
             i <= levels.values().stream().max(Integer::compareTo).orElseThrow(); i++) {
            System.out.println(i + ": " + levelsAggregation.get(i).stream().map(pw -> ((PipelineWorker) pw).getName())
                    .collect(Collectors.toList()));
        }
    }

    private void classifyPipelineWorkers() {
        outputComponents = pipeline.getPipelineWorkers().stream().filter(pw -> pw instanceof OutputComponent)
                .map(pw -> (OutputComponent<?>) pw).collect(Collectors.toSet());
        inputComponents = pipeline.getPipelineWorkers().stream().filter(pw -> pw instanceof InputComponent)
                .map(pw -> (InputComponent<?>) pw).collect(Collectors.toSet());
        forks = pipeline.getPipelineWorkers().stream().filter(pw -> pw instanceof Fork).map(pw -> (Fork<?>) pw)
                .collect(Collectors.toSet());
        joins = pipeline.getPipelineWorkers().stream().filter(pw -> pw instanceof Join).map(pw -> (Join<?>) pw)
                .collect(Collectors.toSet());
    }

    private void setLevel(Object pipelineWorker, int level) {
        if (!levelsAggregation.containsKey(level))
            levelsAggregation.put(level, new ArrayList<>(1));
        if (!levelsAggregation.get(level).contains(pipelineWorker))
            levelsAggregation.get(level).add(pipelineWorker);
        levels.put(pipelineWorker, level);
    }

    private void setConnectionsLevel(Object pipelineWorker, int level) {
        if (pipelineWorker instanceof OutputComponent)
            setOutputTargetsLevel(((OutputComponent<?>) pipelineWorker).getOutput(), level);
        if (pipelineWorker instanceof InputComponent)
            setInputSourcesLevel(((InputComponent<?>) pipelineWorker).getInput(), level);
        if (pipelineWorker instanceof Fork)
            setForkTargetsLevel(Arrays.asList(((Fork<?>) pipelineWorker).getOutputs()), level);
        else if (pipelineWorker instanceof Join)
            setJoinSourcesLevel(Arrays.asList(((Join<?>) pipelineWorker).getInputs()), level);
    }

    private void setOutputTargetsLevel(Pipe<?> output, int sourceLevel) {
        inputComponents.stream().filter(unleveled).filter(ic -> ic.getInput().equals(output)).forEach(
                ic -> setLevel(ic, sourceLevel + 1));
        joins.stream().filter(unleveled).filter(j -> Arrays.asList(j.getInputs()).contains(output)).forEach(
                j -> setLevel(j, sourceLevel + 1));
    }

    private void setInputSourcesLevel(Pipe<?> input, int targetLevel) {
        outputComponents.stream().filter(unleveled).filter(oc -> oc.getOutput().equals(input)).forEach(
                oc -> setLevel(oc, targetLevel - 1));
        forks.stream().filter(unleveled).filter(f -> Arrays.asList(f.getOutputs()).contains(input)).forEach(
                f -> setLevel(f, targetLevel - 1));
    }

    private void setForkTargetsLevel(List<Pipe<?>> outputs, int forkLevel) {
        inputComponents.stream().filter(unleveled).filter(ic -> outputs.contains(ic.getInput())).forEach(
                ic -> setLevel(ic, forkLevel + 1));
    }

    private void setJoinSourcesLevel(List<Pipe<?>> inputs, int joinLevel) {
        outputComponents.stream().filter(unleveled).filter(oc -> inputs.contains(oc.getOutput())).forEach(
                oc -> setLevel(oc, joinLevel - 1));
    }
}
