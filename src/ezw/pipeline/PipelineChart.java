package ezw.pipeline;

import ezw.concurrent.LazyRunnable;
import ezw.util.ElasticMatrix;
import ezw.util.Sugar;

import java.util.*;

class PipelineChart {
    private final List<PipelineWorker> pipelineWorkers;
    private final ElasticMatrix<Object> matrix = new ElasticMatrix<>();
    private final Set<PipelineWarning> warnings = new LinkedHashSet<>(1);
    private final Map<Pipe<?>, List<OutputWorker<?>>> outputSuppliers = new HashMap<>();
    private final Map<Pipe<?>, List<InputWorker<?>>> inputConsumers = new HashMap<>();
    private Set<Join<?>> joins;

    PipelineChart(List<PipelineWorker> pipelineWorkers, SupplyPipe<?> supplyPipe) {
        this.pipelineWorkers = pipelineWorkers;
        classifyComponents();
        var suppliers = outputSuppliers.get(supplyPipe);
        if (suppliers != null)
            matrix.addColumn(suppliers.toArray());
        matrix.addColumn(supplyPipe);
        next();
        pack();
        if (!pipelineWorkers.stream().allMatch(matrix::contains))
            warnings.add(PipelineWarning.DISCOVERY);
    }

    private void classifyComponents() {
        Set<OutputWorker<?>> outputWorkers = Sugar.instancesOf(pipelineWorkers, OutputWorker.class);
        outputWorkers.forEach(ow -> outputSuppliers.compute(ow.getOutput(), (pipe, workers) -> {
            if (workers == null) {
                return new ArrayList<>(List.of(ow));
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
                            matrix.set(indexOfJoin, null);
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
        if (index != null)
            matrix.set(index, null);
    }

    private void pack() {
        if (matrix.isEmpty())
            return;
        for (int y = 2; y < matrix.size().getY(); y++) {
            List<ElasticMatrix.Coordinates> blocks = new ArrayList<>();
            int blockStart = -1;
            for (int x = 1; x < matrix.size().getX(); x++) {
                if (blockStart == -1 && matrix.get(x, y) != null && matrix.get(x - 1, y) == null &&
                        matrix.get(x, y - 1) == null) {
                    blockStart = x;
                } else if (blockStart != -1 && matrix.get(x, y) == null && matrix.get(x, y - 1) == null) {
                    blocks.add(ElasticMatrix.Coordinates.of(blockStart, x));
                    blockStart = -1;
                } else if (blockStart != -1 && matrix.get(x, y - 1) != null) {
                    blockStart = -1;
                }
            }
            if (blockStart != -1)
                blocks.add(ElasticMatrix.Coordinates.of(blockStart, matrix.size().getX() - 1));
            int fy = y;
            blocks.forEach(block -> {
                for (int x = block.getX(); x <= block.getY(); x++) {
                    matrix.swap(x, fy, x, fy - 1);
                }
            });
        }
        var lastRow = matrix.getLastRow();
        while (lastRow.stream().allMatch(Objects::isNull)) {
            matrix.removeRow(matrix.size().getY() - 1);
            lastRow = matrix.getLastRow();
        }
    }

    Set<PipelineWarning> getWarnings() {
        return warnings;
    }

    @Override
    public String toString() {
        return matrix.isEmpty() ? "No chart available." : matrix.toString();
    }
}
