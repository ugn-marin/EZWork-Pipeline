package ezw.pipeline.workers;

import ezw.pipeline.Pipe;
import ezw.pipeline.PipeFunction;

public class CharToIntFunction extends PipeFunction<Character, Integer> {

    public CharToIntFunction(Pipe<Character> input, Pipe<Integer> output, int parallel) {
        super(input, output, parallel);
    }

    @Override
    public Integer apply(Character item) {
        return (int) item;
    }
}
