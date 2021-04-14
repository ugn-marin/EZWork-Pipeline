package ezw.pipeline.workers;

import ezw.pipeline.Pipe;
import ezw.pipeline.Function;

public class CharUpperFunction extends Function<Character, Character> {

    public CharUpperFunction(Pipe<Character> input, Pipe<Character> output, int parallel) {
        super(input, output, parallel);
    }

    @Override
    protected Character apply(Character item) throws InterruptedException {
        return Character.toUpperCase(item);
    }
}
