package ezw.pipeline;

import ezw.util.Sugar;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

class PipelineChartBuilder implements Callable<String> {
    private final List<PipelineWorker> pipelineWorkers;
    private final Set<Object> leveledWorkers;
    private final Map<Integer, List<Object>> levelsAggregation;
    private final Predicate<Object> notLeveledCheck;
    private final Set<PipelineWarning> warnings;
    private final Map<Pipe<?>, Integer> pipeRows;
    private final Set<Pipe<?>> outputPipes;
    private final Map<Pipe<?>, Integer> pipeOutputLevels;
    private final Map<Pipe<?>, Integer> pipeInputLevels;

    private Set<OutputComponent<?>> outputComponents;
    private Set<InputComponent<?>> inputComponents;
    private Set<Fork<?>> forks;
    private Set<Join<?>> joins;
    private int minLevel;
    private int maxLevel;
    private int maxLevelSize;
    private Object[][] chartMatrix;

    PipelineChartBuilder(List<PipelineWorker> pipelineWorkers) {
        this.pipelineWorkers = pipelineWorkers;
        leveledWorkers = new HashSet<>(pipelineWorkers.size());
        levelsAggregation = new HashMap<>(2);
        notLeveledCheck = Predicate.not(leveledWorkers::contains);
        warnings = new HashSet<>(1);
        pipeRows = new HashMap<>(pipelineWorkers.size());
        outputPipes = new HashSet<>(pipelineWorkers.size());
        pipeOutputLevels = new HashMap<>(pipelineWorkers.size());
        pipeInputLevels = new HashMap<>(pipelineWorkers.size());
    }

    Set<PipelineWarning> getWarnings() {
        return warnings;
    }

    @Override
    public String call() throws UnsupportedOperationException {
        classifyPipelineWorkers();
        setLevel(Sugar.first(pipelineWorkers), 0);
        if (leveledWorkers.size() < pipelineWorkers.size())
            warnings.add(PipelineWarning.DISCOVERY);
        summarizeLevels();
        chartMatrix = new Object[levelsAggregation.size()][maxLevelSize];
        Map<Object, String> toString = new HashMap<>(pipelineWorkers.size());
        int[] levelsLength = new int[levelsAggregation.size()];
        buildChartMatrix(toString, levelsLength);
        return getChart(toString, levelsLength);
    }

    private void classifyPipelineWorkers() {
        outputComponents = Sugar.instancesOf(pipelineWorkers, OutputComponent.class);
        inputComponents = Sugar.instancesOf(pipelineWorkers, InputComponent.class);
        forks = Sugar.instancesOf(pipelineWorkers, Fork.class);
        joins = Sugar.instancesOf(pipelineWorkers, Join.class);
    }

    private void setLevel(Object pipelineWorker, int level) {
        if (!levelsAggregation.containsKey(level))
            levelsAggregation.put(level, new ArrayList<>(1));
        if (!levelsAggregation.get(level).contains(pipelineWorker))
            levelsAggregation.get(level).add(pipelineWorker);
        leveledWorkers.add(pipelineWorker);
        setConnectionsLevel(pipelineWorker, level);
    }

    private void setConnectionsLevel(Object pipelineWorker, int level) {
        if (pipelineWorker instanceof OutputComponent)
            setOutputTargetsLevel((OutputComponent<?>) pipelineWorker, level);
        if (pipelineWorker instanceof InputComponent)
            setInputSourcesLevel((InputComponent<?>) pipelineWorker, level);
        if (pipelineWorker instanceof Fork)
            setForkTargetsLevel((Fork<?>) pipelineWorker, level);
        else if (pipelineWorker instanceof Join)
            setJoinSourcesLevel((Join<?>) pipelineWorker, level);
    }

    private void setOutputTargetsLevel(OutputComponent<?> outputComponent, int sourceLevel) {
        Pipe<?> output = outputComponent.getOutput();
        inputComponents.stream().filter(notLeveledCheck).filter(ic -> ic.getInput().equals(output)).forEach(
                ic -> setLevel(ic, sourceLevel + 1));
        joins.stream().filter(notLeveledCheck).filter(j -> Arrays.asList(j.getInputs()).contains(output)).forEach(
                j -> setLevel(j, sourceLevel + 1));
    }

    private void setInputSourcesLevel(InputComponent<?> inputComponent, int targetLevel) {
        Pipe<?> input = inputComponent.getInput();
        outputComponents.stream().filter(notLeveledCheck).filter(oc -> oc.getOutput().equals(input)).forEach(
                oc -> setLevel(oc, targetLevel - 1));
        forks.stream().filter(notLeveledCheck).filter(f -> Arrays.asList(f.getOutputs()).contains(input)).forEach(
                f -> setLevel(f, targetLevel - 1));
    }

    private void setForkTargetsLevel(Fork<?> fork, int forkLevel) {
        List<Pipe<?>> outputs = Arrays.asList(fork.getOutputs());
        inputComponents.stream().filter(notLeveledCheck).filter(ic -> outputs.contains(ic.getInput())).forEach(
                ic -> setLevel(ic, forkLevel + 1));
    }

    private void setJoinSourcesLevel(Join<?> join, int joinLevel) {
        List<Pipe<?>> inputs = Arrays.asList(join.getInputs());
        outputComponents.stream().filter(notLeveledCheck).filter(oc -> inputs.contains(oc.getOutput())).forEach(
                oc -> setLevel(oc, joinLevel - 1));
    }

    private void summarizeLevels() {
        minLevel = levelsAggregation.keySet().stream().min(Integer::compareTo).orElseThrow();
        maxLevel = levelsAggregation.keySet().stream().max(Integer::compareTo).orElseThrow();
        maxLevelSize = levelsAggregation.values().stream().mapToInt(List::size).max().orElseThrow();
    }

    private void buildChartMatrix(Map<Object, String> toString, int[] levelsLength) {
        for (int level = minLevel; level <= maxLevel; level++) {
            List<Object> pws = levelsAggregation.get(level);
            pws.sort(Comparator.comparing(Object::toString));
            Map<Integer, Integer> rowSwaps = new HashMap<>(maxLevelSize / 2);
            for (int row = 0; row < pws.size(); row++) {
                Object pw = pws.get(row);
                String pwStr = getString(pw, level, row, rowSwaps);
                toString.put(pw, pwStr);
                if (pwStr.length() > levelsLength[level - minLevel])
                    levelsLength[level - minLevel] = pwStr.length();
                chartMatrix[level - minLevel][row] = pw;
            }
            doLevelSwaps(level, rowSwaps);
        }
    }

    private String getString(Object pipelineWorker, int level, int row, Map<Integer, Integer> rowSwaps) {
        String pwStr = pipelineWorker.toString();
        int newRow = row;
        Pipe<?> pipe;
        if (pipelineWorker instanceof OutputComponent) {
            pipe = ((OutputComponent<?>) pipelineWorker).getOutput();
            if (!pipeRows.containsKey(pipe))
                pwStr = String.format("%s %s", pwStr, pipe);
            setPipeRow(pipe, row);
            detectPipeCycle(pipe, pipeOutputLevels, level);
            if (!outputPipes.add(pipe))
                warnings.add(PipelineWarning.MULTIPLE_INPUTS);
        }
        if (pipelineWorker instanceof InputComponent) {
            pipe = ((InputComponent<?>) pipelineWorker).getInput();
            if (!pipeRows.containsKey(pipe))
                pwStr = String.format("%s %s", pipe, pwStr);
            newRow = setPipeRow(pipe, row);
            detectPipeCycle(pipe, pipeInputLevels, level);
        }
        if (newRow != row)
            rowSwaps.put(newRow, row);
        return pwStr;
    }

    private Integer setPipeRow(Pipe<?> pipe, Integer row) {
        if (pipeRows.containsKey(pipe))
            row = pipeRows.get(pipe);
        else
            pipeRows.put(pipe, row);
        return row;
    }

    private void detectPipeCycle(Pipe<?> pipe, Map<Pipe<?>, Integer> pipeLevels, int level) {
        if (warnings.contains(PipelineWarning.CYCLE))
            return;
        if (pipeLevels.containsKey(pipe) && level != pipeLevels.get(pipe))
            warnings.add(PipelineWarning.CYCLE);
        pipeLevels.put(pipe, level);
    }

    private void doLevelSwaps(int level, Map<Integer, Integer> rowSwaps) {
        for (var rowSwap : rowSwaps.entrySet()) {
            Object temp = chartMatrix[level - minLevel][rowSwap.getKey()];
            chartMatrix[level - minLevel][rowSwap.getKey()] = chartMatrix[level - minLevel][rowSwap.getValue()];
            chartMatrix[level - minLevel][rowSwap.getValue()] = temp;
        }
    }

    private String getChart(Map<Object, String> toString, int[] levelsLength) {
        StringBuilder lines = new StringBuilder();
        for (int i = 0; i < maxLevelSize; i++) {
            StringBuilder line = new StringBuilder();
            for (int level = minLevel; level <= maxLevel; level++) {
                Object pw = chartMatrix[level - minLevel][i];
                int length = levelsLength[level - minLevel];
                if (level > minLevel)
                    line.append(" ");
                if (pw == null) {
                    line.append(" ".repeat(length));
                } else {
                    String pwStr = toString.get(pw);
                    if (pwStr.contains(" -<"))
                        pwStr = pwStr.replace(" -<", " ".repeat(length - pwStr.length() + 1) + "-<");
                    line.append(pwStr.replace("  ", " -").replace("- -", "---"))
                            .append(" ".repeat(length - pwStr.length()));
                }
            }
            lines.append(line.toString().stripTrailing().replace("- -", "---")).append('\n');
        }
        return lines.toString().stripTrailing();
    }
}
