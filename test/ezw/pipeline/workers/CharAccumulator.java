package ezw.pipeline.workers;

import ezw.pipeline.PipeConsumer;
import ezw.pipeline.Pipe;

public class CharAccumulator extends PipeConsumer<Character> {
    private final StringBuilder sb = new StringBuilder();

    public CharAccumulator(Pipe<Character> input, int parallel) {
        super(input, parallel);
    }

    @Override
    public void accept(Character item) throws InterruptedException {
        synchronized (sb) {
            sb.append(item);
        }
    }

    public String getValue() {
        return sb.toString();
    }
}
