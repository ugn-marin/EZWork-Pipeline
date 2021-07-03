package ezw.pipeline.workers;

import ezw.pipeline.Function;
import ezw.pipeline.Pipe;

public class IntToCharFunction extends Function<Integer, Character> {

    public IntToCharFunction(Pipe<Integer> input, Pipe<Character> output, int parallel) {
        super(input, output, parallel);
    }

    @Override
    public Character apply(Integer item) {
        return (char) item.intValue();
    }
}
