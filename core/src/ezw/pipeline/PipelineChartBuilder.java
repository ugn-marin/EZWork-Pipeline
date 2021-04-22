package ezw.pipeline;

import ezw.util.Sugar;
import ezw.util.generic.Tuple;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

class PipelineChartBuilder implements Callable<String> {
    private final List<PipelineWorker> pipelineWorkers;
    private final Set<Object> leveledWorkers;
    private final Map<Integer, List<Object>> levelsAggregation;
    private final Predicate<Object> notLeveledCheck;

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
    }

    @Override
    public String call() throws UnsupportedOperationException {
        classifyPipelineWorkers();
        setLevel(Sugar.Collections.first(pipelineWorkers), 0);
        if (leveledWorkers.size() < pipelineWorkers.size())
            throw new UnsupportedOperationException("Not all workers are discoverable.");
        summarizeLevels();
        chartMatrix = new Object[levelsAggregation.size()][maxLevelSize];
        Map<Object, String> toString = new HashMap<>(pipelineWorkers.size());
        int[] levelsLength = new int[levelsAggregation.size()];
        buildChartMatrix(toString, levelsLength);
        return getChart(toString, levelsLength);
    }

    private void classifyPipelineWorkers() {
        outputComponents = Sugar.Collections.instancesOf(pipelineWorkers, OutputComponent.class);
        inputComponents = Sugar.Collections.instancesOf(pipelineWorkers, InputComponent.class);
        forks = Sugar.Collections.instancesOf(pipelineWorkers, Fork.class);
        joins = Sugar.Collections.instancesOf(pipelineWorkers, Join.class);
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
        Map<Pipe<?>, Integer> pipeRows = new HashMap<>();
        for (int level = minLevel; level <= maxLevel; level++) {
            List<Object> pws = levelsAggregation.get(level);
            pws.sort(Comparator.comparing(Object::toString));
            List<Tuple<Integer, Integer>> rowSwaps = new ArrayList<>(maxLevelSize / 2);
            for (int row = 0; row < pws.size(); row++) {
                Object pw = pws.get(row);
                Tuple<String, Integer> appendPipeInfo = appendPipes((PipelineWorker) pw, pipeRows, row);
                String pwStr = appendPipeInfo.getFirst();
                if (pwStr.length() > levelsLength[level - minLevel])
                    levelsLength[level - minLevel] = pwStr.length();
                if (appendPipeInfo.getSecond() != row)
                    rowSwaps.add(new Tuple<>(appendPipeInfo.getSecond(), row));
                chartMatrix[level - minLevel][row] = pw;
                toString.put(pw, pwStr);
            }
            doLevelSwaps(level, rowSwaps);
        }
    }

    private Tuple<String, Integer> appendPipes(PipelineWorker pw, Map<Pipe<?>, Integer> pipeRows, Integer row) {
        String pwStr = pw.toString();
        Pipe<?> pipe;
        if (pw instanceof OutputComponent) {
            pipe = ((OutputComponent<?>) pw).getOutput();
            if (!pipeRows.containsKey(pipe))
                pwStr = String.format("%s -%s", pwStr, pipe);
            setPipeRow(pipe, pipeRows, row);
        }
        if (pw instanceof InputComponent) {
            pipe = ((InputComponent<?>) pw).getInput();
            if (!pipeRows.containsKey(pipe))
                pwStr = String.format("%s- %s", pipe, pwStr);
            else
                pwStr = String.format("-- %s", pwStr);
            row = setPipeRow(pipe, pipeRows, row);
        }
        return new Tuple<>(pwStr, row);
    }

    private Integer setPipeRow(Pipe<?> pipe, Map<Pipe<?>, Integer> pipeRows, Integer row) {
        if (pipeRows.containsKey(pipe))
            row = pipeRows.get(pipe);
        else
            pipeRows.put(pipe, row);
        return row;
    }

    private void doLevelSwaps(int level, List<Tuple<Integer, Integer>> rowSwaps) {
        for (var rowSwap : rowSwaps) {
            Object temp = chartMatrix[level - minLevel][rowSwap.getFirst()];
            chartMatrix[level - minLevel][rowSwap.getFirst()] = chartMatrix[level - minLevel][rowSwap.getSecond()];
            chartMatrix[level - minLevel][rowSwap.getSecond()] = temp;
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
                    if (pwStr.contains(" --<"))
                        pwStr = pwStr.replace(" --<", " ".repeat(length - pwStr.length() + 1) + "--<");
                    line.append(pwStr).append(" ".repeat(length - pwStr.length()));
                }
            }
            lines.append(line.toString().stripTrailing().replace(">- -- ", ">---- ")).append('\n');
        }
        return lines.toString().stripTrailing();
    }
}
