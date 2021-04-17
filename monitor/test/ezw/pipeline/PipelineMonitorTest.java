package ezw.pipeline;

import ezw.pipeline.workers.CharAccumulator;
import ezw.pipeline.workers.CharLowerFunction;
import ezw.pipeline.workers.CharSupplier;
import ezw.pipeline.workers.CharUpperFunction;
import org.junit.jupiter.api.Test;

public class PipelineMonitorTest {

    @Test
    void analyze() {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(1);
        CharSupplier charSupplier = new CharSupplier("", supplyPipe, 1);
        var builder = Pipeline.from(charSupplier);

        Pipe<Character> toUpper = new Pipe<>(2);
        Pipe<Character> toLower = new Pipe<>(5);
        Pipe<Character> toIdentity = new Pipe<>(5);
        builder = builder.fork(supplyPipe, toUpper, toLower, toIdentity);

        Pipe<Character> upper = new Pipe<>(1);
        CharUpperFunction charUpperFunction = new CharUpperFunction(toUpper, upper, 1);
        Pipe<Character> lower = new Pipe<>(6);
        CharLowerFunction charLowerFunction = new CharLowerFunction(toLower, lower, 1);
        ezw.pipeline.Function<Character, Character> identity = Pipelines.function(toIdentity, new Pipe<>(1),
                java.util.function.Function.identity());
        builder = builder.through(charLowerFunction, charUpperFunction, identity);

        Pipe<Character> mix = new Pipe<>(7);
        CharAccumulator charAccumulator = new CharAccumulator(mix, 1);

        builder = builder.join(charAccumulator, charUpperFunction, charLowerFunction, identity);

        var pipeline = builder.into(charAccumulator);
        new Analyzer(pipeline).analyze();
    }
}
