package ezw.pipeline.workers;

import ezw.pipeline.Consumer;
import ezw.pipeline.Pipe;

public class CharAccumulator extends Consumer<Character> {
    private final StringBuilder sb = new StringBuilder();

    public CharAccumulator(Pipe<Character> input, int parallel) {
        super(input, parallel);
    }

    @Override
    protected void accept(Character item) throws InterruptedException {
        synchronized (sb) {
            sb.append(item);
        }
    }

    public String getValue() {
        return sb.toString();
    }
}
