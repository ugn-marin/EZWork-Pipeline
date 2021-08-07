package ezw.pipeline.workers;

import ezw.pipeline.Pipe;
import ezw.pipeline.PipeFunction;

public class IntToCharFunction extends PipeFunction<Integer, Character> {

    public IntToCharFunction(Pipe<Integer> input, Pipe<Character> output, int parallel) {
        super(input, output, parallel);
    }

    @Override
    public Character apply(Integer item) {
        return (char) item.intValue();
    }
}
