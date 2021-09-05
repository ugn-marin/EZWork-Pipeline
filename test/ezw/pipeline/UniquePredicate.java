package ezw.pipeline;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;

/**
 * A predicate returning true for arguments having a hash code not previously encountered by the predicate. For a large
 * number of arguments it will be necessary to use the <code>clear(t)</code> method.
 * @param <T> The arguments type.
 */
public class UniquePredicate<T> implements Predicate<T> {
    private final Set<Integer> hashCodes = new ConcurrentSkipListSet<>();

    /**
     * Returns true if the argument's hash code is unique among the cached hash codes previously encountered by this
     * predicate, else false.
     */
    @Override
    public boolean test(T t) {
        return t != null && hashCodes.add(t.hashCode());
    }

    /**
     * Clears the argument's hash code from this predicate's cached hash codes. Returns true if existed in the cache.
     * Use this method when the predicate is expected to encounter a large number of arguments: Clear the arguments as
     * soon as they're handled by the system and are sure not to be encountered again, to avoid running out of memory.
     */
    public boolean clear(T t) {
        return t != null && hashCodes.remove(t.hashCode());
    }

    /**
     * Clears this predicate's cached hash codes.
     */
    public void clear() {
        hashCodes.clear();
    }
}
