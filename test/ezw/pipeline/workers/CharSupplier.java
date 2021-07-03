package ezw.pipeline.workers;

import ezw.pipeline.Supplier;
import ezw.pipeline.SupplyPipe;

import java.util.concurrent.atomic.AtomicInteger;

public class CharSupplier extends Supplier<Character> {
    private final String text;
    private final AtomicInteger index = new AtomicInteger();

    public CharSupplier(String text, SupplyPipe<Character> output, int parallel) {
        super(output, parallel);
        this.text = text;
    }

    @Override
    public Character get() throws InterruptedException {
        try {
            return text.charAt(index.getAndIncrement());
        } catch (StringIndexOutOfBoundsException e) {
            return null;
        }
    }
}
