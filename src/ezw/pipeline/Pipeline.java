package ezw.pipeline;

import ezw.util.Sugar;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A directed acyclic graph (DAG) of concurrent workers connected by pipes. May start at a supplier (closed pipeline) or
 * a supply pipe (open pipeline) and go through a number of functions and transformers. Ends with one or more consumers.
 * @param <S> The type of items supplied at the start of the pipeline.
 */
public final class Pipeline<S> extends PipelineWorker implements SupplyGate<S> {
    private final List<PipelineWorker> pipelineWorkers;
    private final SupplyPipe<S> supplyPipe;
    private final Set<PipelineWarning> pipelineWarnings;
    private final String toString;

    /**
     * Constructs a builder of a closed pipeline, and attaches the suppliers provided.
     * @param suppliers One or more suppliers feeding the pipeline.
     * @param <S> The type of the supplied items.
     * @return The pipeline builder.
     */
    @SafeVarargs
    public static <S> Builder<S> from(Supplier<S>... suppliers) {
        return new Builder<>(suppliers);
    }

    /**
     * Constructs a builder of an open pipeline.
     * @param supplyPipe The supply pipe feeding the pipeline.
     * @param <S> The type of the supplied items.
     * @return The pipeline builder.
     */
    public static <S> Builder<S> from(SupplyPipe<S> supplyPipe) {
        return new Builder<>(supplyPipe);
    }

    private Pipeline(List<PipelineWorker> pipelineWorkers, SupplyPipe<S> supplyPipe) {
        super(pipelineWorkers.size());
        this.pipelineWorkers = pipelineWorkers;
        this.supplyPipe = supplyPipe;
        boolean isOpen = pipelineWorkers.stream().noneMatch(pw -> pw instanceof Supplier);
        int connectorsCount = Sugar.instancesOf(pipelineWorkers, PipeConnector.class).size();
        StringBuilder sb = new StringBuilder(String.format("%s of %d workers on %d working threads:%n", isOpen ?
                "Open pipeline" : "Pipeline", pipelineWorkers.size() - connectorsCount, getWorkersParallel()));
        var pipelineChartBuilder = new PipelineChartBuilder(pipelineWorkers);
        sb.append(pipelineChartBuilder.get());
        pipelineWarnings = pipelineChartBuilder.getWarnings();
        for (PipelineWarning warning : pipelineWarnings) {
            sb.append(System.lineSeparator()).append(warning.getDescription());
        }
        toString = sb.toString();
    }

    /**
     * Returns the maximum number of auto-allocated threads that this pipeline's workers can use. That doesn't include
     * the threads managing and joining the workers, and the various pipe connectors.
     */
    public int getWorkersParallel() {
        return pipelineWorkers.stream().mapToInt(PipelineWorker::getParallel).sum();
    }

    @Override
    public int getCancelledWork() {
        return pipelineWorkers.stream().mapToInt(PipelineWorker::getCancelledWork).sum();
    }

    @Override
    protected void work() {
        pipelineWorkers.forEach(this::submit);
    }

    /**
     * Pushes an item into the supply pipe feeding the pipeline. This is the entry point of an open pipeline, although
     * might be used for additional supply for a closed pipeline as well, as long as end of input wasn't reached.
     * @param item The item.
     * @throws InterruptedException If interrupted while attempting to push the item.
     */
    @Override
    public void push(S item) throws InterruptedException {
        supplyPipe.push(item);
    }

    @Override
    protected void onFinish(Throwable throwable) throws Exception {
        setEndOfInput();
        pipelineWorkers.forEach(pipelineWorker -> pipelineWorker.cancel(throwable));
        Sugar.<InputComponent<?>>instancesOf(pipelineWorkers, InputComponent.class).stream()
                .map(InputComponent::getInput).forEach(Pipe::clear);
        Sugar.<OutputComponent<?>>instancesOf(pipelineWorkers, OutputComponent.class).stream()
                .map(OutputComponent::getOutput).forEach(Pipe::clear);
        super.onFinish(throwable);
    }

    /**
     * Marks end of input for the pipeline supply. The pipeline will stop after all workers finish handling the items
     * already in the pipes. Use for open pipelines only: If the pipeline has suppliers, they may fail on trying to push
     * after end of input.
     */
    public void setEndOfInput() {
        supplyPipe.setEndOfInput();
    }

    /**
     * Returns the warnings detected on the pipeline construction.
     */
    public Set<PipelineWarning> getWarnings() {
        return new LinkedHashSet<>(pipelineWarnings);
    }

    @Override
    public String toString() {
        return toString;
    }

    /**
     * A pipeline builder.
     * @param <S> The type of items supplied at the start of the pipeline.
     */
    public static final class Builder<S> {
        private final List<PipelineWorker> pipelineWorkers = new ArrayList<>();
        private final SupplyPipe<S> supplyPipe;

        @SafeVarargs
        private Builder(Supplier<S>... suppliers) {
            var supplyPipes = Arrays.stream(Sugar.requireFull(suppliers)).map(Supplier::getOutput)
                    .collect(Collectors.toSet());
            if (supplyPipes.size() != 1)
                throw new IllegalArgumentException("The pipeline suppliers must feed exactly 1 supply pipe.");
            supplyPipe = (SupplyPipe<S>) supplyPipes.stream().findFirst().get();
            attach(suppliers);
        }

        private Builder(SupplyPipe<S> supplyPipe) {
            this.supplyPipe = Objects.requireNonNull(supplyPipe, "Supply pipe cannot be null.");
        }

        /**
         * Attaches one or more functions to the pipeline.
         * @param functions One or more functions.
         * @param <I> The input items type.
         * @param <O> The output items type.
         * @return This builder.
         */
        @SafeVarargs
        public final <I, O> Builder<S> through(Function<I, O>... functions) {
            return attach(functions);
        }

        /**
         * Attaches one or more transformers to the pipeline.
         * @param transformers One or more transformers.
         * @param <I> The input items type.
         * @param <O> The output items type.
         * @return This builder.
         */
        @SafeVarargs
        public final <I, O> Builder<S> through(Transformer<I, O>... transformers) {
            return attach(transformers);
        }

        /**
         * Creates a fork from the input pipe into the output pipes.
         * @param input The input pipe.
         * @param outputs The output pipes.
         * @param <I> The items type.
         * @return This builder.
         */
        @SafeVarargs
        public final <I> Builder<S> fork(Pipe<I> input, Pipe<I>... outputs) {
            return attach(new Fork<>(input, outputs));
        }

        /**
         * Creates a fork from the input pipe into the input components (outputs of the fork). This does not attach the
         * components.
         * @param input The input pipe.
         * @param outputs The input components.
         * @param <I> The items type.
         * @return This builder.
         */
        @SafeVarargs
        @SuppressWarnings("unchecked")
        public final <I> Builder<S> fork(Pipe<I> input, InputComponent<I>... outputs) {
            return fork(input, Arrays.stream(Sugar.requireFull(outputs)).map(InputComponent::getInput)
                    .toArray(Pipe[]::new));
        }

        /**
         * Creates a fork from the output component (input of the fork) into the input components (outputs of the fork).
         * This does not attach the components.
         * @param input The output component.
         * @param outputs The input components.
         * @param <I> The items type.
         * @return This builder.
         */
        @SafeVarargs
        public final <I> Builder<S> fork(OutputComponent<I> input, InputComponent<I>... outputs) {
            return fork(input.getOutput(), outputs);
        }

        /**
         * Creates a join from the input pipes into the output pipe.
         * @param output The output pipe.
         * @param inputs The input pipes.
         * @param <I> The items type.
         * @return This builder.
         */
        @SafeVarargs
        public final <I> Builder<S> join(Pipe<I> output, Pipe<I>... inputs) {
            return attach(new Join<>(output, inputs));
        }

        /**
         * Creates a join from the output components (inputs of the join) into the output pipe. This does not attach the
         * components.
         * @param output The output pipe.
         * @param inputs The output components.
         * @param <I> The items type.
         * @return This builder.
         */
        @SafeVarargs
        @SuppressWarnings("unchecked")
        public final <I> Builder<S> join(Pipe<I> output, OutputComponent<I>... inputs) {
            return join(output, Arrays.stream(Sugar.requireFull(inputs)).map(OutputComponent::getOutput)
                    .toArray(Pipe[]::new));
        }

        /**
         * Creates a join from the output components (inputs of the join) into the input component (output of the join).
         * This does not attach the components.
         * @param output The input component.
         * @param inputs The output components.
         * @param <I> The items type.
         * @return This builder.
         */
        @SafeVarargs
        public final <I> Builder<S> join(InputComponent<I> output, OutputComponent<I>... inputs) {
            return join(output.getInput(), inputs);
        }

        /**
         * Attaches one or more consumers to the pipeline.
         * @param consumers One or more consumers.
         * @return The pipeline.
         */
        public Pipeline<S> into(Consumer<?>... consumers) {
            return new Pipeline<>(attach(consumers).pipelineWorkers, supplyPipe);
        }

        private Builder<S> attach(PipelineWorker... pipelineWorkers) {
            this.pipelineWorkers.addAll(List.of(Sugar.requireFull(pipelineWorkers)));
            return this;
        }
    }
}
