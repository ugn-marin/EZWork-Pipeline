package ezw.pipeline;

import ezw.concurrent.Concurrent;
import ezw.concurrent.Interruptible;
import ezw.pipeline.workers.*;
import ezw.util.Sugar;
import ezw.util.function.UniquePredicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

public class PipelineTest {
    private static final String ABC = "ABCD-EFG-HIJK-LMNOP-QRS-TUV-WXYZ";
    private static final String abc = ABC.toLowerCase();
    private static final String digits = "0123456789";
    private static final String full = String.format("The average person should be able to read this sentence, " +
            "as well as know the ABC which is: %s, and know the digits: %s.", ABC, digits);
    private static final String five = String.format("1. %s\n2. %s\n3. %s\n4. %s\n5. %s", full, full, full, full, full);
    private static final int minimumCapacity = 1;
    private static final int smallCapacity = 10;
    private static final int mediumCapacity = 100;
    private static final int largeCapacity = 1000;
    private static final Random random = new Random();

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        System.out.println(testInfo.getDisplayName());
    }

    @AfterEach
    void afterEach() {
        System.out.println();
    }

    private static void sleep(int millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private static void sleepBetween(int min, int max) throws InterruptedException {
        sleep(min + random.nextInt(max - min));
    }

    private static String switchCase(String text) {
        StringBuilder sb = new StringBuilder();
        for (char c : text.toCharArray()) {
            sb.append(Character.isLowerCase(c) ? Character.toUpperCase(c) : Character.toLowerCase(c));
        }
        return sb.toString();
    }

    private static void validate(Pipeline<?> pipeline) {
        System.out.println(pipeline);
        assertTrue(pipeline.getWarnings().isEmpty(), "Warnings found");
    }

    @Test
    void constructor_definitions() {
        SupplyPipe<Object> pipe = new SupplyPipe<>(1);
        PipelineWorker worker = Pipelines.supplier(pipe, () -> null);
        assertEquals(1, worker.getConcurrency());
        worker = Pipelines.supplier(pipe, 5, () -> null);
        assertEquals(5, worker.getConcurrency());
        worker = Pipelines.function(pipe, pipe, x -> null);
        assertEquals(1, worker.getConcurrency());
        worker = Pipelines.function(pipe, pipe, 5, x -> null);
        assertEquals(5, worker.getConcurrency());
        worker = Pipelines.transformer(pipe, pipe, x -> null, () -> null);
        assertEquals(1, worker.getConcurrency());
        worker = Pipelines.transformer(pipe, pipe, 5, x -> null, () -> null);
        assertEquals(5, worker.getConcurrency());
        worker = Pipelines.consumer(pipe, x -> {});
        assertEquals(1, worker.getConcurrency());
        worker = Pipelines.consumer(pipe, 5, x -> {});
        assertEquals(5, worker.getConcurrency());
    }

    @Test
    void validations() throws Exception {
        // Range
        try {
            new Pipe<Integer>(0);
            fail();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
        // Nulls
        try {
            Pipelines.supplier(null, () -> null);
            fail();
        } catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
        try {
            Pipelines.function(null, null, x -> null);
            fail();
        } catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
        try {
            Pipelines.transformer(null, null, x -> null, () -> null);
            fail();
        } catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
        try {
            Pipelines.consumer(null, x -> {});
            fail();
        } catch (NullPointerException e) {
            System.out.println(e.getMessage());
        }
        // Lists
        SupplyPipe<Object> pipe = new SupplyPipe<>(1);
        try {
            Pipelines.star(pipe);
            fail();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
        try {
            Pipeline.from(pipe).join(pipe, pipe);
            fail();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
        try {
            Pipeline.from(pipe).into();
            fail();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
        try {
            Pipeline.from(Pipelines.supplier(new SupplyPipe<>(1), () -> null),
                    Pipelines.supplier(new SupplyPipe<>(1), () -> null));
            fail();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
        // End of input
        try {
            var pipeline = Pipelines.direct(() -> null, x -> {});
            System.out.println(pipeline);
            pipeline.run();
            pipeline.push(1);
            fail();
        } catch (IllegalStateException e) {
            System.out.println(e.getMessage());
        }
        // Reuse
        try {
            var pipeline = Pipelines.direct(() -> null, x -> {});
            System.out.println(pipeline);
            pipeline.run();
            pipeline.run();
            fail();
        } catch (IllegalStateException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    void chart_disconnected() {
        var pipeline = Pipelines.direct(Pipelines.supplier(new SupplyPipe<Integer>(1), () -> null),
                Pipelines.consumer(new Pipe<>(1), x -> {}));
        System.out.println(pipeline);
        assertTrue(pipeline.getWarnings().contains(PipelineWarning.DISCOVERY));
        assertTrue(pipeline.toString().contains(PipelineWarning.DISCOVERY.getDescription()));
    }

    @Test
    void chart_cycle() {
        var supplyPipe = new SupplyPipe<Character>(5);
        var pipeline = Pipeline.from(new CharSupplier(abc, supplyPipe, 1))
                .through(Pipelines.function(supplyPipe, supplyPipe, x -> x))
                .into(new Printer<>(System.out, supplyPipe, 1));
        System.out.println(pipeline);
        assertTrue(pipeline.getWarnings().contains(PipelineWarning.CYCLE));
    }

    @Test
    void never_started_stop() {
        var pipeline = Pipelines.direct(() -> null, x -> {});
        System.out.println(pipeline);
        pipeline.stop();
    }

    @Test
    void never_started_interrupt() {
        var pipeline = Pipelines.direct(() -> null, x -> {});
        System.out.println(pipeline);
        pipeline.interrupt();
    }

    @Test
    void supplier1_minimum_accumulator1() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(minimumCapacity);
        CharSupplier charSupplier = new CharSupplier(abc, supplyPipe, 1);
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 1);
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(abc, charAccumulator.getValue());
        assertTrue(pipeline.toString().contains("Pipeline of 2 workers on 2 working threads:"));
    }

    @Test
    void supplier1_large_accumulator1() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(largeCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 1);
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(five, charAccumulator.getValue());
    }

    @Test
    void supplier1_lower1_upper1_accumulator1() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(mediumCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        Pipe<Character> lower = new Pipe<>(smallCapacity);
        CharLowerFunction charLowerFunction = new CharLowerFunction(supplyPipe, lower, 1);
        Pipe<Character> upper = new Pipe<>(smallCapacity);
        CharUpperFunction charUpperFunction = new CharUpperFunction(lower, upper, 1);
        CharAccumulator charAccumulator = new CharAccumulator(upper, 1);
        var pipeline = Pipeline.from(charSupplier).through(charLowerFunction, charUpperFunction).into(charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(five.toUpperCase(), charAccumulator.getValue());
    }

    @Test
    void supplier1_lower10_upper10_accumulator1() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(mediumCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        Pipe<Character> lower = new Pipe<>(smallCapacity);
        CharLowerFunction charLowerFunction = new CharLowerFunction(supplyPipe, lower, 1);
        Pipe<Character> upper = new Pipe<>(smallCapacity);
        CharUpperFunction charUpperFunction = new CharUpperFunction(lower, upper, 1);
        CharAccumulator charAccumulator = new CharAccumulator(upper, 1);
        var pipeline = Pipeline.from(charSupplier).through(charLowerFunction, charUpperFunction).into(charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(five.toUpperCase(), charAccumulator.getValue());
    }

    @Test
    void supplier1_minimum_accumulator1slow() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(minimumCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 5);
                super.accept(item);
            }
        };
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(five, charAccumulator.getValue());
    }

    @Test
    void supplier1_large_accumulator1slow() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(largeCapacity);
        CharSupplier charSupplier = new CharSupplier(full, supplyPipe, 1);
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(10, 20);
                super.accept(item);
            }
        };
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(full, charAccumulator.getValue());
    }

    @Test
    void supplier1_large_accumulator10slow() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(largeCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 10) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 5);
                super.accept(item);
            }
        };
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(five.length(), charAccumulator.getValue().length());
    }

    @Test
    void supplier1_large_accumulator1slow_checkPipe() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(largeCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 5);
                super.accept(item);
            }
        };
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        var future = Concurrent.calculate(pipeline);
        sleep(200);
        assertTrue(supplyPipe.totalItems() > mediumCapacity);
        future.get();
        assertEquals(five, charAccumulator.getValue());
    }

    @Test
    void supplier20slow_small_accumulator5slow() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(smallCapacity);
        CharSupplier charSupplier = new CharSupplier(five.repeat(5), supplyPipe, 20) {
            @Override
            public Character get() throws InterruptedException {
                sleepBetween(1, 5);
                return super.get();
            }
        };
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 5) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 3);
                super.accept(item);
            }
        };
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(five.length() * 5, charAccumulator.getValue().length());
        assertTrue(pipeline.toString().contains("Pipeline of 2 workers on 25 working threads:"));
    }

    @Test
    void supplier10slow_medium_accumulator5slow() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(mediumCapacity);
        CharSupplier charSupplier = new CharSupplier(five.repeat(5), supplyPipe, 10) {
            @Override
            public Character get() throws InterruptedException {
                sleepBetween(1, 5);
                return super.get();
            }
        };
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 5) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 3);
                super.accept(item);
            }
        };
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(five.length() * 5, charAccumulator.getValue().length());
    }

    @Test
    void supplier32slow_minimum_accumulator10slow() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(minimumCapacity);
        CharSupplier charSupplier = new CharSupplier(five.repeat(5), supplyPipe, 32) {
            @Override
            public Character get() throws InterruptedException {
                sleepBetween(1, 10);
                return super.get();
            }
        };
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 10) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 10);
                super.accept(item);
            }
        };
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(five.length() * 5, charAccumulator.getValue().length());
    }

    @Test
    void supplier32slow_medium_accumulator10slow_interrupt() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(mediumCapacity);
        CharSupplier charSupplier = new CharSupplier(five.repeat(5), supplyPipe, 32) {
            @Override
            public Character get() throws InterruptedException {
                sleepBetween(1, 10);
                return super.get();
            }
        };
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 10) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 10);
                super.accept(item);
            }
        };
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        Concurrent.calculate(() -> {
            sleep(600);
            pipeline.interrupt();
        });
        try {
            pipeline.run();
            fail("Not interrupted");
        } catch (InterruptedException e) {
            assertEquals("Controlled interruption.", e.getMessage());
        }
        assertTrue(pipeline.getCancelledWork() > 0);
        assertTrue(five.length() * 5 > charAccumulator.getValue().length());
        assertEquals(0, supplyPipe.totalItems());
    }

    @Test
    void supplier1_large_fork_accumulator1slow_print() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(largeCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        CharAccumulator charAccumulator = new CharAccumulator(new Pipe<>(smallCapacity), 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 5);
                super.accept(item);
            }
        };
        var printer = new Printer<>(System.out, new Pipe<Character>(mediumCapacity), 1);
        var pipeline = Pipelines.star(charSupplier, charAccumulator, printer);
        validate(pipeline);
        pipeline.run();
        assertEquals(five, charAccumulator.getValue());
        assertTrue(pipeline.toString().contains("Pipeline of 3 workers on 3 working threads:"));
    }

    @Test
    void supplier1_fork_upper1_lower1_join_accumulator1() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(smallCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        var builder = Pipeline.from(charSupplier);

        Pipe<Character> toUpper = new Pipe<>(smallCapacity);
        Pipe<Character> toLower = new Pipe<>(mediumCapacity);
        builder = builder.fork(supplyPipe, toUpper, toLower);

        Pipe<Character> upper = new Pipe<>(smallCapacity);
        CharUpperFunction charUpperFunction = new CharUpperFunction(toUpper, upper, 1);
        Pipe<Character> lower = new Pipe<>(mediumCapacity);
        CharLowerFunction charLowerFunction = new CharLowerFunction(toLower, lower, 1);
        builder = builder.through(charLowerFunction, charUpperFunction);

        Pipe<Character> mix = new Pipe<>(mediumCapacity);
        CharAccumulator charAccumulator = new CharAccumulator(mix, 1);

        builder = builder.join(charAccumulator, charUpperFunction, charLowerFunction);

        var pipeline = builder.into(charAccumulator);
        validate(pipeline);
        assertEquals(4, pipeline.getWorkersConcurrency());
        pipeline.run();
        assertEquals(switchCase(five), charAccumulator.getValue());
    }

    @Test
    void supplier1_fork_upper1_lower1_identity1_join_accumulator1() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(smallCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        var builder = Pipeline.from(charSupplier);

        Pipe<Character> toUpper = new Pipe<>(smallCapacity);
        Pipe<Character> toLower = new Pipe<>(mediumCapacity);
        Pipe<Character> toIdentity = new Pipe<>(mediumCapacity);
        SupplyPipe<Character> hyphens = new SupplyPipe<>(largeCapacity, c -> c == '-');
        builder = builder.fork(supplyPipe, toUpper, toLower, toIdentity, hyphens);

        Pipe<Character> upper = new Pipe<>(smallCapacity);
        CharUpperFunction charUpperFunction = new CharUpperFunction(toUpper, upper, 1);
        Pipe<Character> lower = new Pipe<>(mediumCapacity);
        CharLowerFunction charLowerFunction = new CharLowerFunction(toLower, lower, 1);
        PipeFunction<Character, Character> identity = Pipelines.function(toIdentity, new Pipe<>(smallCapacity),
                Function.identity());
        Pipe<Character> toPrint = new Pipe<>(minimumCapacity);
        builder = builder.through(charLowerFunction, charUpperFunction, identity, Pipelines.function(hyphens, toPrint,
                Function.identity()));

        Pipe<Character> mix = new Pipe<>(mediumCapacity);
        CharAccumulator charAccumulator = new CharAccumulator(mix, 1);

        builder = builder.join(charAccumulator, charUpperFunction, charLowerFunction, identity);

        var pipeline = builder.into(charAccumulator, new Printer<>(System.out, toPrint, 2));
        validate(pipeline);
        assertEquals(8, pipeline.getWorkersConcurrency());
        pipeline.run();
        assertEquals(switchCase(five), charAccumulator.getValue());
        assertEquals(five.length(), supplyPipe.getItemsPushed());
        assertEquals(five.length(), upper.getItemsPushed());
        assertEquals(five.length(), lower.getItemsPushed());
        assertEquals(five.length(), toLower.getItemsPushed());
        assertEquals(five.length(), toIdentity.getItemsPushed());
        assertEquals(30, hyphens.getItemsPushed());
    }

    @Test
    void supplier1_fork_upper3slow_lower3slower_join_fork_accumulator1slow_print() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(largeCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        var builder = Pipeline.from(charSupplier);

        Pipe<Character> toUpper = new Pipe<>(smallCapacity);
        Pipe<Character> toLower = new Pipe<>(mediumCapacity);
        builder = builder.fork(supplyPipe, toUpper, toLower);

        Pipe<Character> upper = new Pipe<>(smallCapacity);
        CharUpperFunction charUpperFunction = new CharUpperFunction(toUpper, upper, 3) {
            @Override
            public Character apply(Character item) throws InterruptedException {
                sleepBetween(2, 8);
                return super.apply(item);
            }
        };
        Pipe<Character> lower = new Pipe<>(mediumCapacity);
        CharLowerFunction charLowerFunction = new CharLowerFunction(toLower, lower, 3) {
            @Override
            public Character apply(Character item) throws InterruptedException {
                sleepBetween(10, 15);
                return super.apply(item);
            }
        };
        builder = builder.through(charLowerFunction, charUpperFunction);

        Pipe<Character> mix = new Pipe<>(mediumCapacity);
        builder = builder.join(mix, upper, lower);

        Pipe<Character> toAccum = new Pipe<>(smallCapacity);
        Pipe<Character> toPrint = new Pipe<>(smallCapacity);
        CharAccumulator charAccumulator = new CharAccumulator(toAccum, 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 5);
                super.accept(item);
            }
        };
        var printer = new Printer<>(System.out, toPrint, 1);

        var pipeline = builder.fork(mix, toAccum, toPrint).into(charAccumulator, printer);
        validate(pipeline);
        assertEquals(9, pipeline.getWorkersConcurrency());
        pipeline.run();
        // Join prefers modified - here different case
        assertEquals(switchCase(five), charAccumulator.getValue());
        assertEquals(0, pipeline.getCancelledWork());
        assertEquals(five.length(), supplyPipe.getItemsPushed());
        assertEquals(five.length(), upper.getItemsPushed());
        assertEquals(five.length(), lower.getItemsPushed());
    }

    @Test
    void supplier1_large_fork_accumulator2slow_print_fail() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(largeCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        Pipe<Character> toAccum = new Pipe<>(smallCapacity);
        Pipe<Character> toPrint = new Pipe<>(mediumCapacity);
        CharAccumulator charAccumulator = new CharAccumulator(toAccum, 2) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 5);
                if (item == 'Z')
                    throw new NumberFormatException("My failure message");
                super.accept(item);
            }
        };
        var printer = new Printer<>(System.out, toPrint, 1);
        Pipeline<Character> pipeline = null;
        try {
            pipeline = Pipelines.star(charSupplier, charAccumulator, printer);
            validate(pipeline);
            pipeline.run();
            fail("Not failed");
        } catch (NumberFormatException e) {
            assertEquals("My failure message", e.getMessage());
        }
        assert pipeline != null;
        assertTrue(pipeline.getCancelledWork() > 0);
    }

    @Test
    void supplier1_large_fork_accumulator2slow_print_cancel() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(largeCapacity);
        CharSupplier charSupplier = new CharSupplier(five, supplyPipe, 1);
        Pipe<Character> toAccum = new Pipe<>(smallCapacity);
        Pipe<Character> toPrint = new Pipe<>(mediumCapacity);
        CharAccumulator charAccumulator = new CharAccumulator(toAccum, 2) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(100, 500);
                super.accept(item);
            }
        };
        var printer = new Printer<>(System.out, toPrint, 1);
        var pipeline = Pipelines.star(charSupplier, charAccumulator, printer);
        validate(pipeline);
        Concurrent.calculate(() -> {
            sleep(600);
            pipeline.cancel(new NumberFormatException("My cancellation message"));
        });
        try {
            pipeline.run();
            fail("Not cancelled");
        } catch (NumberFormatException e) {
            assertEquals("My cancellation message", e.getMessage());
        }
        assertTrue(pipeline.getCancelledWork() > 0);
    }

    @Test
    void supplier1conditional_minimum_accumulator1() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(minimumCapacity, c -> c != '-');
        CharSupplier charSupplier = new CharSupplier(abc, supplyPipe, 1);
        CharAccumulator charAccumulator = new CharAccumulator(supplyPipe, 1);
        var pipeline = Pipelines.direct(charSupplier, charAccumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(abc.replace("-", ""), charAccumulator.getValue());
    }

    @Test
    void stream_conditional_accumulator1() throws Exception {
        var supplyPipe = new SupplyPipe<>(minimumCapacity, (Predicate<Character>) Character::isAlphabetic);
        var supplier = Pipelines.supplier(supplyPipe, abc.chars().mapToObj(c -> (char) c));
        var accumulator = new CharAccumulator(supplyPipe, 1);
        var pipeline = Pipelines.direct(supplier, accumulator);
        validate(pipeline);
        pipeline.run();
        assertEquals(abc.replace("-", ""), accumulator.getValue());
    }

    @Test
    void supplier1_transformer1_fork_printer_counter() throws Exception {
        var supplier = new CharSupplier(five, new SupplyPipe<>(largeCapacity), 1);
        var transformer = new WordsTransformer(supplier.getOutput(), new SupplyPipe<>(mediumCapacity), 1);
        final AtomicInteger wordsCount = new AtomicInteger();
        Pipe<String> toAccum = new Pipe<>(smallCapacity);
        Pipe<String> toPrint = new Pipe<>(mediumCapacity);
        var consumer = Pipelines.consumer(toAccum, s -> wordsCount.incrementAndGet());
        var printer = new Printer<>(System.out, toPrint, 1);
        Pipeline.from(supplier).through(transformer).fork(transformer, consumer, printer).into(consumer, printer).run();
        assertEquals(125, wordsCount.get());
        assertEquals(125, toAccum.getItemsPushed());
        assertEquals(125, toPrint.getItemsPushed());
        assertEquals(five.length(), supplier.getOutput().getItemsPushed());
    }

    @Test
    void supplier1_transformer1conditional_fork_printer_counter() throws Exception {
        var supplier = new CharSupplier(five, new SupplyPipe<>(largeCapacity), 1);
        var transformer = new WordsTransformer(supplier.getOutput(), new SupplyPipe<>(mediumCapacity, s -> s.length() <= 2), 1);
        final AtomicInteger wordsCount = new AtomicInteger();
        Pipe<String> toAccum = new Pipe<>(smallCapacity);
        Pipe<String> toPrint = new Pipe<>(mediumCapacity);
        var consumer = Pipelines.consumer(toAccum, s -> wordsCount.incrementAndGet());
        var printer = new Printer<>(System.out, toPrint, 1);
        var pipeline = Pipeline.from(supplier).through(transformer).fork(transformer, consumer, printer).into(consumer, printer);
        validate(pipeline);
        pipeline.run();
        assertEquals(25, wordsCount.get());
    }

    @Test
    void conditional_function() throws Exception {
        var supplier = new CharSupplier(full, new SupplyPipe<>(minimumCapacity), 1);
        var function = Pipelines.function(supplier.getOutput(),
                new SupplyPipe<Character>(minimumCapacity, Character::isUpperCase), Function.identity());
        var consumer = new CharAccumulator(function.getOutput(), 1);
        var pipeline = Pipeline.from(supplier).through(function).into(consumer);
        validate(pipeline);
        pipeline.run();
        assertEquals("TABC" + ABC.replace("-", ""), consumer.getValue());
    }

    @Test
    void conditional_fork() throws Exception {
        var supplier = new CharSupplier(abc, new SupplyPipe<>(mediumCapacity), 1);
        var accum1 = new CharAccumulator(new SupplyPipe<>(mediumCapacity, c -> c == '-'), 1);
        var accum2 = new CharAccumulator(new SupplyPipe<>(minimumCapacity, c -> c != '-'), 1);
        var pipeline = Pipelines.star(supplier, accum1, accum2);
        validate(pipeline);
        pipeline.run();
        assertEquals("------", accum1.getValue());
        assertEquals(abc.replace("-", ""), accum2.getValue());
    }

    @Test
    void conditional_direct() throws Exception {
        var supplier = new CharSupplier(abc, new SupplyPipe<>(minimumCapacity, c -> c != '-'), 1);
        var accum = new CharAccumulator(supplier.getOutput(), 1);
        var pipeline = Pipelines.direct(supplier, accum);
        validate(pipeline);
        pipeline.run();
        assertEquals(abc.replace("-", ""), accum.getValue());
    }

    @Test
    void conditional_direct_two_suppliers() throws Exception {
        SupplyPipe<Character> supplyPipe = new SupplyPipe<>(minimumCapacity, c -> c == '-');
        var supplier1 = new CharSupplier(abc, supplyPipe, 1);
        var supplier2 = new CharSupplier(abc, supplyPipe, 1);
        var accum = new CharAccumulator(supplyPipe, 1);
        var pipeline = Pipeline.from(supplier1, supplier2).into(accum);
        System.out.println(pipeline);
        assertTrue(pipeline.getWarnings().contains(PipelineWarning.MULTIPLE_INPUTS));
        pipeline.run();
        assertEquals("-".repeat(12), accum.getValue());
        assertEquals(12, supplyPipe.getItemsPushed());
        assertEquals(0, pipeline.getCancelledWork());
    }

    @Test
    void unique_direct() throws Exception {
        var supplier = new CharSupplier(full, new SupplyPipe<>(smallCapacity, new UniquePredicate<>()), 1);
        var accum = new CharAccumulator(supplier.getOutput(), 1);
        var pipeline = Pipelines.direct(supplier, accum);
        validate(pipeline);
        pipeline.run();
        assertEquals("The avrgpsonuldbtic,wkABC:D-EFGHIJKLMNOPQRSUVWXYZ0123456789.", accum.getValue());
    }

    @Test
    void unique_direct_parallel() throws Exception {
        var supplier = new CharSupplier(five, new SupplyPipe<>(smallCapacity, new UniquePredicate<>()), 4);
        var accum = new CharAccumulator(supplier.getOutput(), 2);
        var pipeline = Pipelines.direct(supplier, accum);
        validate(pipeline);
        pipeline.run();
        assertEquals("The avrgpsonuldbtic,wkABC:D-EFGHIJKLMNOPQRSUVWXYZ0123456789.".length() + 1,
                accum.getValue().length());
    }

    @Test
    void mixed_fork() throws Exception {
        var supplier = new CharSupplier(abc, new SupplyPipe<>(mediumCapacity), 1);
        var accum1 = new CharAccumulator(new SupplyPipe<>(mediumCapacity, c -> c == '-'), 1);
        var accum2 = new CharAccumulator(new Pipe<>(minimumCapacity), 1);
        var pipeline = Pipelines.star(supplier, accum1, accum2);
        validate(pipeline);
        pipeline.run();
        assertEquals("------", accum1.getValue());
        assertEquals(abc, accum2.getValue());
    }

    @Test
    void conditional_fork_slow() throws Exception {
        var supplier = new CharSupplier(abc, new SupplyPipe<>(minimumCapacity), 1) {
            @Override
            public Character get() throws InterruptedException {
                sleepBetween(1, 300);
                return super.get();
            }
        };
        var accum1 = new CharAccumulator(new SupplyPipe<>(minimumCapacity, c -> c == '-'), 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 300);
                super.accept(item);
            }
        };
        var accum2 = new CharAccumulator(new SupplyPipe<>(smallCapacity, c -> c != '-'), 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 300);
                super.accept(item);
            }
        };
        var pipeline = Pipelines.star(supplier, accum1, accum2);
        validate(pipeline);
        pipeline.run();
        assertEquals("------", accum1.getValue());
        assertEquals(abc.replace("-", ""), accum2.getValue());
    }

    @Test
    void open_star_slow() throws Exception {
        var consumer = new CharAccumulator(new Pipe<>(smallCapacity), 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 20);
                super.accept(item);
            }
        };
        var printer = new Printer<>(System.out, new Pipe<Character>(smallCapacity), 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleepBetween(1, 50);
                super.accept(item);
            }
        };
        final var pipeline = Pipelines.star(new SupplyPipe<>(largeCapacity), consumer, printer);
        validate(pipeline);
        Concurrent.calculate(() -> {
            for (char c : full.toCharArray()) {
                pipeline.push(c);
            }
            pipeline.setEndOfInput();
        });
        pipeline.run();
        assertEquals(full, consumer.getValue());
        assertEquals(0, pipeline.getCancelledWork());
    }

    @Test
    void open_star_slow_stop() throws Exception {
        var consumer = new CharAccumulator(new Pipe<>(smallCapacity), 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleep(2);
                super.accept(item);
            }
        };
        var printer = new Printer<>(System.out, new Pipe<Character>(smallCapacity), 1) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleep(2);
                super.accept(item);
            }
        };
        final var pipeline = Pipelines.star(new SupplyPipe<>(largeCapacity), consumer, printer);
        validate(pipeline);
        Concurrent.calculate(() -> {
            for (char c : full.toCharArray()) {
                pipeline.push(c);
            }
            sleep(100);
            pipeline.stop();
        });
        pipeline.run();
        assertTrue(full.startsWith(consumer.getValue()));
        assertTrue(pipeline.getCancelledWork() > 0);
    }

    @Test
    void open_star_slow_stop_count() throws Exception {
        var consumer = new CharAccumulator(new Pipe<>(smallCapacity), 10) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleep(2000);
                super.accept(item);
            }
        };
        var printer = new Printer<>(System.out, new Pipe<Character>(smallCapacity), 10) {
            @Override
            public void accept(Character item) throws InterruptedException {
                sleep(2000);
                super.accept(item);
            }
        };
        final var pipeline = Pipelines.star(new SupplyPipe<>(mediumCapacity), consumer, printer);
        validate(pipeline);
        Concurrent.calculate(() -> {
            for (char c : full.toCharArray()) {
                pipeline.push(c);
            }
            sleep(1000);
            pipeline.stop();
        });
        assertEquals(20, pipeline.getWorkersConcurrency());
        pipeline.run();
        assertEquals(20, pipeline.getCancelledWork());
    }

    @Test
    void open_star_push_nulls() throws Exception {
        var consumer = new CharAccumulator(new Pipe<>(smallCapacity), 1);
        var printer = new Printer<>(System.out, new Pipe<Character>(smallCapacity), 1);
        final var pipeline = Pipelines.star(new SupplyPipe<>(largeCapacity), consumer, printer);
        validate(pipeline);
        Concurrent.calculate(() -> {
            var sgc = pipeline.toConsumer();
            for (char c : full.toCharArray()) {
                sgc.accept(c);
                sgc.accept(null);
            }
            pipeline.setEndOfInput();
        });
        pipeline.run();
        assertEquals(full.length() * 5, consumer.getValue().length());
        assertEquals(full, Sugar.remove(consumer.getValue(), "null"));
        assertEquals(0, pipeline.getCancelledWork());
    }

    @Test
    void split() throws Exception {
        final var even = new StringBuilder();
        final var odd = new StringBuilder();
        final var pipeline = Pipelines.<Integer>split(n -> n % 2 == 0, even::append, odd::append);
        validate(pipeline);
        Concurrent.calculate(() -> {
            for (int i = 0; i < 10; i++) {
                pipeline.push(i);
            }
            pipeline.setEndOfInput();
        });
        pipeline.run();
        assertEquals("02468", even.toString());
        assertEquals("13579", odd.toString());
    }

    @Test
    void skip_level() throws Exception {
        var supplyPipe = new SupplyPipe<Character>(smallCapacity);
        var supplier = new CharSupplier(full, supplyPipe, 1);
        var builder = Pipeline.from(supplier);
        var supplied1 = new Pipe<Character>(smallCapacity);
        var supplied2 = new Pipe<Character>(smallCapacity);
        var supplied3 = new Pipe<Character>(smallCapacity);
        var supplied4 = new Pipe<Character>(smallCapacity);
        builder.extend(supplied2).fork(supplyPipe, supplied1, supplied2, supplied3, supplied4);
        var supplied1f = new Pipe<Character>(smallCapacity);
        var supplied3a = new Pipe<Character>(smallCapacity);
        var f = Pipelines.function(supplied1, supplied1f, Character::toUpperCase);
        var a = Pipelines.action(supplied3, supplied3a, 6, (Consumer<Character>) Interruptible::sleep);
        builder.through(f, a);
        var words = new SupplyPipe<String>(mediumCapacity, word -> word.length() < 3);
        var wordsTrans = new WordsTransformer(supplied4, words);
        builder.through(wordsTrans);
        var joined = new Pipe<Character>(minimumCapacity);
        builder.join(joined, supplied1f, supplied2, supplied3a);
        var joinedAccum = new CharAccumulator(joined, 1);
        var wordsPrinter = new Printer<>(System.out, words, 1);
        var pipeline = builder.into(joinedAccum, wordsPrinter);
        validate(pipeline);
        pipeline.run();
        assertEquals(full.length(), joinedAccum.getValue().length());
        assertNotEquals(full, joinedAccum.getValue());
    }

    @Test
    void skip_level_no_extension() throws Exception {
        var supplyPipe = new SupplyPipe<Character>(smallCapacity);
        var supplier = new CharSupplier(full, supplyPipe, 1);
        var builder = Pipeline.from(supplier);
        var supplied1 = new Pipe<Character>(smallCapacity);
        var supplied2 = new Pipe<Character>(smallCapacity);
        var supplied3 = new Pipe<Character>(smallCapacity);
        builder.fork(supplyPipe, supplied1, supplied2, supplied3);
        var supplied1f = new Pipe<Character>(smallCapacity);
        var f = Pipelines.function(supplied1, supplied1f, Character::toUpperCase);
        builder.through(f);
        var words = new SupplyPipe<String>(mediumCapacity);
        var wordsTrans = new WordsTransformer(supplied3, words);
        builder.through(wordsTrans);
        var joined = new Pipe<Character>(minimumCapacity);
        builder.join(joined, supplied1f, supplied2);
        var joinedAccum = new CharAccumulator(joined, 1);
        var wordsPrinter = new Printer<>(System.out, words, 1);
        var pipeline = builder.into(joinedAccum, wordsPrinter);
        assertTrue(pipeline.getWarnings().contains(PipelineWarning.EXTENSION));
        pipeline.run();
        assertEquals(full.length(), joinedAccum.getValue().length());
        assertNotEquals(full, joinedAccum.getValue());
    }
}
