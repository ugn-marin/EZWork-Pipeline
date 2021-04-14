package ezw.pipeline.workers;

import ezw.pipeline.Pipe;
import ezw.pipeline.Function;

public class IntToCharFunction extends Function<Integer, Character> {

    public IntToCharFunction(Pipe<Integer> input, Pipe<Character> output, int parallel) {
        super(input, output, parallel);
    }

    @Override
    protected Character apply(Integer item) {
        return (char) item.intValue();
    }
}
