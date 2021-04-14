package ezw.pipeline.workers;

import ezw.pipeline.Pipe;
import ezw.pipeline.Function;

public class CharToIntFunction extends Function<Character, Integer> {

    public CharToIntFunction(Pipe<Character> input, Pipe<Integer> output, int parallel) {
        super(input, output, parallel);
    }

    @Override
    protected Integer apply(Character item) {
        return (int) item;
    }
}
