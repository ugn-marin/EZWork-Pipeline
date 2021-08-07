package ezw.pipeline.workers;

import ezw.pipeline.Pipe;
import ezw.pipeline.SupplyPipe;
import ezw.pipeline.PipeTransformer;

import java.util.Collection;
import java.util.List;

public class WordsTransformer extends PipeTransformer<Character, String> {
    private final StringBuilder sb = new StringBuilder();

    public WordsTransformer(Pipe<Character> input, SupplyPipe<String> output) {
        super(input, output);
    }

    public WordsTransformer(Pipe<Character> input, SupplyPipe<String> output, int parallel) {
        super(input, output, parallel);
    }

    @Override
    public Collection<String> apply(Character item) {
        if (item == ' ' || item == '\n')
            return nextWord(item == '\n');
        sb.append(item);
        return null;
    }

    @Override
    protected Collection<String> getLastItems() {
        if (sb.length() > 0)
            return nextWord(true);
        return null;
    }

    private Collection<String> nextWord(boolean endOfLine) {
        String word = sb.toString();
        sb.delete(0, sb.length());
        if (endOfLine)
            word += '\n';
        return List.of(word);
    }
}
