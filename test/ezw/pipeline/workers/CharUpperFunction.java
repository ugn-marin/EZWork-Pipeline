package ezw.pipeline.workers;

import ezw.pipeline.Pipe;
import ezw.pipeline.PipeFunction;

public class CharUpperFunction extends PipeFunction<Character, Character> {

    public CharUpperFunction(Pipe<Character> input, Pipe<Character> output, int parallel) {
        super(input, output, parallel);
    }

    @Override
    public Character apply(Character item) throws InterruptedException {
        return Character.toUpperCase(item);
    }
}
