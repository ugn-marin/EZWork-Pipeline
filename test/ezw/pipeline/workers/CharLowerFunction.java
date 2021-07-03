package ezw.pipeline.workers;

import ezw.pipeline.Function;
import ezw.pipeline.Pipe;

public class CharLowerFunction extends Function<Character, Character> {

    public CharLowerFunction(Pipe<Character> input, Pipe<Character> output, int parallel) {
        super(input, output, parallel);
    }

    @Override
    public Character apply(Character item) throws InterruptedException {
        return Character.toLowerCase(item);
    }
}
