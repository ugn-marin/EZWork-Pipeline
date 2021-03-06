package ezw.pipeline.workers;

import ezw.pipeline.Pipe;
import ezw.pipeline.PipeConsumer;

import java.io.PrintStream;

public class Printer<I> extends PipeConsumer<I> {
    private final PrintStream ps;

    public Printer(PrintStream ps, Pipe<I> input, int parallel) {
        super(input, parallel);
        this.ps = ps;
    }

    @Override
    public void accept(I item) throws InterruptedException {
        ps.print(item);
    }

    @Override
    protected void close() {
        ps.println();
        ps.flush();
    }
}
