package ezw.pipeline.workers;

import ezw.pipeline.Consumer;
import ezw.pipeline.Pipe;

import java.io.PrintStream;

public class Printer<I> extends Consumer<I> {
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
    protected void onFinish(Throwable throwable) throws Exception {
        ps.flush();
        super.onFinish(throwable);
    }
}
