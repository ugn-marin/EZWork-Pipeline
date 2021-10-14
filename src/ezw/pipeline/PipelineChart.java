package ezw.pipeline;

import ezw.Sugar;
import ezw.concurrent.LazyRunnable;
import ezw.data.Matrix;
import ezw.data.Range;

import java.util.*;

class PipelineChart {
    private final List<PipelineWorker> pipelineWorkers;
    private final SupplyPipe<?> supplyPipe;
    private final Matrix<Object> matrix = new Matrix<>();
    private final Set<PipelineWarning> warnings = new LinkedHashSet<>(1);
    private final Map<Pipe<?>, List<OutputWorker<?>>> outputSuppliers = new HashMap<>();
    private final Map<Pipe<?>, List<InputWorker<?>>> inputConsumers = new HashMap<>();
    private Set<Fork<?>> forks;
    private Set<Join<?>> joins;

    PipelineChart(List<PipelineWorker> pipelineWorkers, SupplyPipe<?> supplyPipe) {
        this.pipelineWorkers = pipelineWorkers;
        this.supplyPipe = supplyPipe;
        classifyComponents();
        var suppliers = outputSuppliers.get(supplyPipe);
        int suppliersCount = suppliers != null ? suppliers.size() : 0;
        if (suppliersCount > 0)
            matrix.addColumn(suppliers.toArray());
        matrix.addColumn(supplyPipe);
        next();
        if (!matrix.isEmpty()) {
            pack();
            if (suppliersCount != 1)
                supplyLeading(suppliersCount > 1);
            forkOutputsLeading();
            joinInputsTrailing();
            if (!pipelineWorkers.stream().allMatch(matrix::contains))
                warnings.add(PipelineWarning.DISCOVERY);
        }
    }

    private void classifyComponents() {
        Set<OutputWorker<?>> outputWorkers = Sugar.instancesOf(pipelineWorkers, OutputWorker.class);
        outputWorkers.forEach(ow -> outputSuppliers.compute(ow.getOutput(), (pipe, workers) -> {
            if (workers == null) {
                return new ArrayList<>(List.of(ow));
            } else if (!(pipe instanceof SupplyGate)) {
                throw new PipelineConfigurationException("Multiple workers push into the same pipe using different " +
                        "index scopes.");
            } else {
                workers.add(ow);
                workers.sort(Comparator.comparing(Objects::toString));
                warnings.add(PipelineWarning.MULTIPLE_INPUTS);
                return workers;
            }
        }));
        Set<InputWorker<?>> inputWorkers = Sugar.instancesOf(pipelineWorkers, InputWorker.class);
        inputWorkers.forEach(iw -> inputConsumers.compute(iw.getInput(), (pipe, workers) -> {
            if (workers == null) {
                return new ArrayList<>(List.of(iw));
            } else {
                workers.add(iw);
                workers.sort(Comparator.comparing(Objects::toString));
                return workers;
            }
        }));
        forks = Sugar.instancesOf(pipelineWorkers, Fork.class);
        joins = Sugar.instancesOf(pipelineWorkers, Join.class);
    }

    private void next() {
        var level = matrix.getLastColumn();
        int x = matrix.size().getX();
        if (x > pipelineWorkers.size() * 2) {
            warnings.add(PipelineWarning.CYCLE);
            matrix.clear();
            return;
        }
        var addColumn = new LazyRunnable(matrix::addColumn);
        for (Object element : level) {
            if (element == null)
                continue;
            int y = matrix.indexOf(element).getY();
            if (element instanceof Pipe)
                addPipeOutputs(x, y, (Pipe<?>) element, addColumn);
            else if (element instanceof Fork)
                addForkOutputs(x, y, (Fork<?>) element, addColumn);
            else if (element instanceof OutputWorker)
                addOutputPipe(x, y, (OutputWorker<?>) element, addColumn);
        }
        if (addColumn.isCalculated())
            next();
    }

    private void addPipeOutputs(int x, int y, Pipe<?> pipe, Runnable addColumn) {
        var workers = inputConsumers.get(pipe);
        if (workers != null) {
            addPipeWorkers(x, y, workers, addColumn);
        } else {
            addPipeJoin(x, y, pipe, addColumn);
        }
    }

    private void addPipeWorkers(int x, int y, List<InputWorker<?>> workers, Runnable addColumn) {
        Sugar.repeat(workers.size() - 1, () -> matrix.addRowAfter(y));
        addColumn.run();
        int nextY = y;
        for (var worker : workers) {
            clearExtendedComponent(worker);
            matrix.set(x, nextY++, worker);
        }
    }

    private void addPipeJoin(int x, int y, Pipe<?> pipe, Runnable addColumn) {
        var join = joins.stream().filter(j -> List.of(j.getInputs()).contains(pipe)).findFirst();
        if (join.isEmpty())
            return;
        var indexOfJoin = matrix.indexOf(join.get());
        if (indexOfJoin != null) {
            if (indexOfJoin.getX() != x)
                matrix.set(indexOfJoin, "---");
            else
                return;
        }
        addColumn.run();
        matrix.set(x, y, join.get());
    }

    private void addForkOutputs(int x, int y, Fork<?> fork, Runnable addColumn) {
        var outputs = new ArrayList<>(List.of(fork.getOutputs()));
        Sugar.repeat(outputs.size() - 1, () -> matrix.addRowAfter(y));
        addColumn.run();
        outputs.sort(Comparator.comparing(Objects::toString));
        int nextY = y;
        for (var output : outputs) {
            clearExtendedComponent(output);
            matrix.set(x, nextY++, output);
        }
    }

    private void addOutputPipe(int x, int y, OutputWorker<?> worker, Runnable addColumn) {
        var output = worker.getOutput();
        addColumn.run();
        clearExtendedComponent(output);
        matrix.set(x, y, output);
    }

    private void clearExtendedComponent(Object o) {
        var index = matrix.indexOf(o);
        if (index != null) {
            matrix.set(index, null);
            if (o instanceof Pipe)
                matrix.set(index, "-".repeat(matrix.getColumn(index.getX()).stream().filter(Objects::nonNull).mapToInt(
                        c -> Objects.toString(c).length()).max().orElse(3) - 1) + '+');
        }
    }

    private void pack() {
        if (matrix.isEmpty())
            return;
        for (int y = 2; y < matrix.size().getY(); y++) {
            int start = -1;
            for (int x = 1; x < matrix.size().getX(); x++) {
                if (start == -1 && matrix.get(x, y) != null && matrix.get(x - 1, y) == null &&
                        matrix.get(x, y - 1) == null) {
                    start = x;
                } else if (start != -1 && matrix.get(x, y) == null && matrix.get(x, y - 1) == null) {
                    raiseRange(Range.of(start, x + 1), y);
                    start = -1;
                } else if (start != -1 && matrix.get(x, y - 1) != null) {
                    start = -1;
                }
            }
            if (start != -1)
                raiseRange(Range.of(start, matrix.size().getX()), y);
        }
        matrix.packRows();
    }

    private void raiseRange(Range range, int y) {
        range.forEach(x -> matrix.swap(x, y, x, y - 1));
    }

    private void supplyLeading(boolean multiSupply) {
        matrix.set(matrix.indexOf(supplyPipe), supplyPipe.toString().replace("-<", multiSupply ? "*<" : "o<"));
    }

    private void forkOutputsLeading() {
        forks.stream().flatMap(fork -> Arrays.stream(fork.getOutputs())).forEach(pipe ->
                matrix.set(matrix.indexOf(pipe), pipe.toString().replace("-<", "+<")));
    }

    private void joinInputsTrailing() {
        joins.stream().flatMap(join -> Arrays.stream(join.getInputs())).forEach(pipe -> {
            var index = matrix.indexOf(pipe);
            if (index != null) {
                StringBuilder sb = new StringBuilder(pipe.toString());
                sb.append("-".repeat(matrix.getColumn(index.getX()).stream().filter(Objects::nonNull)
                        .mapToInt(c -> Objects.toString(c).length()).max().orElse(sb.length()) - sb.length()));
                matrix.set(index, sb.replace(sb.length() - 1, sb.length(), "+"));
            }
        });
    }

    Set<PipelineWarning> getWarnings() {
        return warnings;
    }

    @Override
    public String toString() {
        if (matrix.isEmpty())
            return "No chart available.";
        StringBuilder sb = new StringBuilder();
        matrix.toString().lines().forEach(line -> {
            StringBuilder lineSB = new StringBuilder(line);
            pipesLeading(lineSB, 0);
            pipesTrailing(lineSB, 0);
            pipesExtensions(lineSB, 0);
            sb.append(lineSB).append(System.lineSeparator());
        });
        return Sugar.replace(sb.toString().stripTrailing(), "- -", "---");
    }

    private void pipesLeading(StringBuilder line, int offset) {
        int to = line.indexOf("  -<", offset);
        if (to > 0) {
            int from = to - 1;
            while (from > offset && line.charAt(from) == ' ') {
                from--;
            }
            if (from > offset && line.charAt(from) != '-')
                line.replace(from + 2, to + 2, "-".repeat(to - from));
            pipesLeading(line, to + 4);
        }
    }

    private void pipesTrailing(StringBuilder line, int offset) {
        int from = line.indexOf(">-  ", offset);
        if (from > 0) {
            int to = from + 4;
            while (to < line.length() && line.charAt(to) == ' ') {
                to++;
            }
            if (to < line.length() && line.charAt(to) != '-') {
                line.replace(from + 2, to - 1, "-".repeat(to - from - 3));
                pipesTrailing(line, to);
            }
        }
    }

    private void pipesExtensions(StringBuilder line, int offset) {
        int from = line.indexOf(" --- ", offset);
        if (from > 0) {
            int to = from + 5;
            while (to < line.length() && line.charAt(to) == ' ') {
                to++;
            }
            if (to + 2 < line.length() && line.indexOf(" ---", to - 1) == to - 1) {
                line.replace(from + 5, to - 1, "-".repeat(to - from - 6));
                pipesExtensions(line, to);
            }
        }
    }
}
