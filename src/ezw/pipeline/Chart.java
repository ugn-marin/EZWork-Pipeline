package ezw.pipeline;

import ezw.util.Sugar;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class Chart implements Runnable {
    private final List<PipelineWorker> pipelineWorkers;
    private final SupplyPipe<?> supplyPipe;

    private Set<OutputWorker<?>> outputWorkers;
    private Map<Pipe<?>, OutputWorker<?>> outputSuppliers;
    private Set<InputWorker<?>> inputWorkers;
    private Map<Pipe<?>, InputWorker<?>> inputConsumers;
    private Set<Fork<?>> forks;
    private Set<Join<?>> joins;

    Chart(List<PipelineWorker> pipelineWorkers, SupplyPipe<?> supplyPipe) {
        this.pipelineWorkers = pipelineWorkers;
        this.supplyPipe = supplyPipe;
    }

    @Override
    public void run() {
        classifyComponents();
    }

    private void classifyComponents() {
        outputWorkers = Sugar.instancesOf(pipelineWorkers, OutputWorker.class);
        outputSuppliers = outputWorkers.stream().collect(Collectors.toMap(OutputWorker::getOutput, ow -> ow));
        inputWorkers = Sugar.instancesOf(pipelineWorkers, InputWorker.class);
        inputConsumers = inputWorkers.stream().collect(Collectors.toMap(InputWorker::getInput, iw -> iw));
        forks = Sugar.instancesOf(pipelineWorkers, Fork.class);
        joins = Sugar.instancesOf(pipelineWorkers, Join.class);
    }
}
