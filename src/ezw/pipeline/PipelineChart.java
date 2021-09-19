package ezw.pipeline;

import ezw.concurrent.LazyRunnable;
import ezw.util.Matrix;
import ezw.util.Sugar;

import java.util.*;
import java.util.stream.Collectors;

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
            rearrangeJoinColumns();
            centerMainRow();
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
        int nextX = matrix.size().getX();
        if (nextX > pipelineWorkers.size() * 2) {
            warnings.add(PipelineWarning.CYCLE);
            matrix.clear();
            return;
        }
        var addColumn = new LazyRunnable(matrix::addColumn);
        for (int y = 0; y < level.size(); y++) {
            var element = level.get(y);
            if (element == null)
                continue;
            int fy = y;
            int nextY = y;
            if (element instanceof Pipe) {
                var workers = inputConsumers.get(element);
                if (workers != null) {
                    Sugar.repeat(workers.size() - 1, () -> matrix.addRowAfter(fy));
                    addColumn.run();
                    for (var worker : workers) {
                        clearExtendedComponent(worker);
                        matrix.set(nextX, nextY++, worker);
                    }
                } else {
                    var join = joins.stream().filter(j -> List.of(j.getInputs()).contains(element)).findFirst();
                    if (join.isEmpty())
                        continue;
                    var indexOfJoin = matrix.indexOf(join.get());
                    if (indexOfJoin != null) {
                        if (indexOfJoin.getX() != nextX)
                            matrix.set(indexOfJoin, "---");
                        else
                            continue;
                    }
                    addColumn.run();
                    matrix.set(nextX, nextY, join.get());
                }
            } else if (element instanceof Fork) {
                var outputs = new ArrayList<>(List.of(((Fork<?>) element).getOutputs()));
                Sugar.repeat(outputs.size() - 1, () -> matrix.addRowAfter(fy));
                addColumn.run();
                outputs.sort(Comparator.comparing(Objects::toString));
                for (var output : outputs) {
                    clearExtendedComponent(output);
                    matrix.set(nextX, nextY++, output);
                }
            } else if (element instanceof OutputWorker) {
                var output = ((OutputWorker<?>) element).getOutput();
                addColumn.run();
                clearExtendedComponent(output);
                matrix.set(nextX, nextY, output);
            }
        }
        if (addColumn.isCalculated())
            next();
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
            List<Matrix.Coordinates> blocks = new ArrayList<>();
            int blockStart = -1;
            for (int x = 1; x < matrix.size().getX(); x++) {
                if (blockStart == -1 && matrix.get(x, y) != null && matrix.get(x - 1, y) == null &&
                        matrix.get(x, y - 1) == null) {
                    blockStart = x;
                } else if (blockStart != -1 && matrix.get(x, y) == null && matrix.get(x, y - 1) == null) {
                    blocks.add(Matrix.Coordinates.of(blockStart, x));
                    blockStart = -1;
                } else if (blockStart != -1 && matrix.get(x, y - 1) != null) {
                    blockStart = -1;
                }
            }
            if (blockStart != -1)
                blocks.add(Matrix.Coordinates.of(blockStart, matrix.size().getX() - 1));
            raiseBlocks(y, blocks);
        }
        matrix.pack(true, false);
    }

    private void raiseBlocks(int y, List<Matrix.Coordinates> blocks) {
        blocks.forEach(block -> {
            for (int x = block.getX(); x <= block.getY(); x++) {
                matrix.swap(x, y, x, y - 1);
            }
        });
    }

    private void rearrangeJoinColumns() {
        matrix.getColumns().stream().filter(column -> column.stream().filter(Objects::nonNull).map(Object::getClass)
                .collect(Collectors.toSet()).contains(Join.class) && column.contains(null)).forEach(column -> {
            List<Integer> nullIndexes = new ArrayList<>();
            for (int y = column.size() - 1; y >= 0; y--) {
                if (column.get(y) == null)
                    nullIndexes.add(y);
            }
            for (int y = 0; y < column.size(); y++) {
                if (nullIndexes.isEmpty())
                    break;
                var o = column.get(y);
                if (o != null && !(o instanceof Join))
                    matrix.swapRows(y, Sugar.removeLast(nullIndexes));
            }
        });
    }

    private void centerMainRow() {
        for (int y = 0; y < (matrix.size().getY() - 1) / 2; y++) {
            matrix.swapRows(y, y + 1);
        }
    }

    private void supplyLeading(boolean multiSupply) {
        matrix.set(matrix.indexOf(supplyPipe), supplyPipe.toString().replace("-<", multiSupply ? "*<" : "o-<"));
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
