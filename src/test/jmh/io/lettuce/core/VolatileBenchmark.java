package io.lettuce.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * x86 VolatileBenchmark.testAtomicBoolean_NewDefault                              avgt    5  10.173 ±  0.157  ms/op
 * VolatileBenchmark.testAtomicBoolean_NewFalse                                avgt    5  10.199 ±  0.071  ms/op
 * VolatileBenchmark.testAtomicBoolean_NewTrue                                 avgt    5  19.945 ±  0.148  ms/op
 * VolatileBenchmark.testAtomicBoolean_Volatile_CompareAndSet_Failed           avgt    5   7.086 ±  0.036  ms/op
 * VolatileBenchmark.testGetAcquire                                            avgt    5   0.187 ±  0.001  ms/op
 * VolatileBenchmark.testGetOpaque                                             avgt    5   0.188 ±  0.001  ms/op
 * VolatileBenchmark.testGetPlain                                              avgt    5   0.024 ±  0.001  ms/op
 * VolatileBenchmark.testGetVolatile                                           avgt    5   0.187 ±  0.001  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetPlain_CompareAndSet_Failed          avgt    5   0.377 ±  0.005  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetPlain_CompareAndSet_Success         avgt    5  12.139 ±  0.068  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetRelease_CompareAndSet_Failed        avgt    5   0.378 ±  0.020  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetRelease_CompareAndSet_Success       avgt    5  12.140 ±  0.035  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetVolatile_WeakCompareAndSet_Failed   avgt    5   0.379 ±  0.006  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetVolatile_WeakCompareAndSet_Success  avgt    5  12.176 ±  0.263  ms/op
 * VolatileBenchmark.testInt_Plain_WeakCompareAndSet_Failed                    avgt    5   7.105 ±  0.042  ms/op
 * VolatileBenchmark.testInt_Plain_WeakCompareAndSet_Success                   avgt    5   7.095 ±  0.021  ms/op
 * VolatileBenchmark.testInt_Volatile_CompareAndSet_Failed                     avgt    5   7.093 ±  0.060  ms/op
 * VolatileBenchmark.testInt_Volatile_CompareAndSet_Success                    avgt    5   7.089 ±  0.021  ms/op
 * VolatileBenchmark.testSetOpaque                                             avgt    5   0.374 ±  0.001  ms/op
 * VolatileBenchmark.testSetPlain                                              avgt    5  ≈ 10⁻⁶           ms/op
 * VolatileBenchmark.testSetRelease                                            avgt    5   0.374 ±  0.002  ms/op
 * VolatileBenchmark.testSetVolatile                                           avgt    5   2.432 ±  0.014  ms/op
 * <p>
 * <p>
 * arm64 VolatileBenchmark.testAtomicBoolean_NewDefault                              avgt    5   9.207 ±  0.378  ms/op
 * VolatileBenchmark.testAtomicBoolean_NewFalse                                avgt    5   8.909 ±  0.171  ms/op
 * VolatileBenchmark.testAtomicBoolean_NewTrue                                 avgt    5   9.332 ±  0.218  ms/op
 * VolatileBenchmark.testAtomicBoolean_Volatile_CompareAndSet_Failed           avgt    5   5.859 ±  0.127  ms/op
 * VolatileBenchmark.testGetAcquire                                            avgt    5   0.208 ±  0.039  ms/op
 * VolatileBenchmark.testGetOpaque                                             avgt    5   0.206 ±  0.017  ms/op
 * VolatileBenchmark.testGetPlain                                              avgt    5   0.033 ±  0.005  ms/op
 * VolatileBenchmark.testGetVolatile                                           avgt    5   0.201 ±  0.001  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetPlain_CompareAndSet_Failed          avgt    5   0.520 ±  0.139  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetPlain_CompareAndSet_Success         avgt    5   9.559 ±  2.472  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetRelease_CompareAndSet_Failed        avgt    5   0.519 ±  0.087  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetRelease_CompareAndSet_Success       avgt    5  11.942 ±  0.120  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetVolatile_WeakCompareAndSet_Failed   avgt    5   0.525 ±  0.086  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetVolatile_WeakCompareAndSet_Success  avgt    5   9.408 ±  0.716  ms/op
 * VolatileBenchmark.testInt_Plain_WeakCompareAndSet_Failed                    avgt    5   6.064 ±  2.090  ms/op
 * VolatileBenchmark.testInt_Plain_WeakCompareAndSet_Success                   avgt    5   7.460 ±  2.117  ms/op
 * VolatileBenchmark.testInt_Volatile_CompareAndSet_Failed                     avgt    5   6.102 ±  2.073  ms/op
 * VolatileBenchmark.testInt_Volatile_CompareAndSet_Success                    avgt    5   5.837 ±  0.364  ms/op
 * VolatileBenchmark.testSetOpaque                                             avgt    5   0.290 ±  0.002  ms/op
 * VolatileBenchmark.testSetPlain                                              avgt    5  ≈ 10⁻⁶           ms/op
 * VolatileBenchmark.testSetRelease                                            avgt    5   4.828 ±  0.127  ms/op
 * VolatileBenchmark.testSetVolatile                                           avgt    5   0.379 ±  0.096  ms/op
 * <p>
 * Mac Pro m2 VolatileBenchmark.testAtomicBoolean_Volatile_CompareAndSet_Failed      avgt    5  17.888 ± 0.729  ms/op
 * VolatileBenchmark.testBoolean_Volatile_CompareAndSet_Failed            avgt    5  17.696 ± 1.740  ms/op
 * VolatileBenchmark.testGetAcquire                                       avgt    5   0.688 ± 0.111  ms/op
 * VolatileBenchmark.testGetOpaque                                        avgt    5   0.108 ± 0.007  ms/op
 * VolatileBenchmark.testGetPlain                                         avgt    5   0.041 ± 0.003  ms/op
 * VolatileBenchmark.testGetVolatile                                      avgt    5   0.679 ± 0.005  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetPlain_CompareAndSet_Failed     avgt    5   0.540 ± 0.047  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetPlain_CompareAndSet_Success    avgt    5   6.110 ± 0.329  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetRelease_CompareAndSet_Failed   avgt    5   0.541 ± 0.034  ms/op
 * VolatileBenchmark.testInt_GetVolatileSetRelease_CompareAndSet_Success  avgt    5   6.833 ± 0.730  ms/op
 * VolatileBenchmark.testInt_Plain_WeakCompareAndSet_Failed               avgt    5  18.040 ± 2.576  ms/op
 * VolatileBenchmark.testInt_Plain_WeakCompareAndSet_Success              avgt    5   2.242 ± 0.208  ms/op
 * VolatileBenchmark.testInt_Volatile_CompareAndSet_Failed                avgt    5  17.248 ± 0.698  ms/op
 * VolatileBenchmark.testInt_Volatile_CompareAndSet_Success               avgt    5   6.687 ± 0.581  ms/op
 * VolatileBenchmark.testSetOpaque                                        avgt    5   9.181 ± 0.330  ms/op
 * VolatileBenchmark.testSetPlain                                         avgt    5   8.920 ± 0.535  ms/op
 * VolatileBenchmark.testSetRelease                                       avgt    5   9.103 ± 0.694  ms/op
 * VolatileBenchmark.testSetVolatile                                      avgt    5   9.509 ± 0.639  ms/op
 */
@State(Scope.Thread)
public class VolatileBenchmark {

    private static final int ITERATIONS = 1_000_000;

    private static final VarHandle A_INT;

    private static final VarHandle AN_ATOMIC_BOOLEAN;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            A_INT = l.findVarHandle(VolatileBenchmark.class, "aInt", int.class);
            AN_ATOMIC_BOOLEAN = l.findVarHandle(VolatileBenchmark.class, "anAtomicBoolean", AtomicBoolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused")
    private int aInt;

    @SuppressWarnings("unused")
    private boolean aBoolean;

    private AtomicBoolean anAtomicBoolean;

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testSetPlain() {
        for (int i = 0; i < ITERATIONS; i++) {
            A_INT.set(this, i);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testSetOpaque() {
        for (int i = 0; i < ITERATIONS; i++) {
            A_INT.setOpaque(this, i);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testSetRelease() {
        for (int i = 0; i < ITERATIONS; i++) {
            A_INT.setRelease(this, i);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testSetVolatile() {
        for (int i = 0; i < ITERATIONS; i++) {
            A_INT.setVolatile(this, i);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testGetPlain(Blackhole blackhole) {
        for (int i = 0; i < ITERATIONS; i++) {
            int value = (int) A_INT.get(this);
            blackhole.consume(value);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testGetOpaque(Blackhole blackhole) {
        for (int i = 0; i < ITERATIONS; i++) {
            int value = (int) A_INT.getOpaque(this);
            blackhole.consume(value);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testGetAcquire(Blackhole blackhole) {
        for (int i = 0; i < ITERATIONS; i++) {
            int value = (int) A_INT.getAcquire(this);
            blackhole.consume(value);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testGetVolatile(Blackhole blackhole) {
        for (int i = 0; i < ITERATIONS; i++) {
            int value = (int) A_INT.getVolatile(this);
            blackhole.consume(value);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_Volatile_CompareAndSet_Success() {
        A_INT.setOpaque(this, 0);
        for (int i = 0; i < ITERATIONS; i++) {
            A_INT.compareAndSet(this, i, i + 1);
        }
        if ((int) A_INT.getOpaque(this) != ITERATIONS) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_Volatile_CompareAndSet_Failed() {
        A_INT.setOpaque(this, Integer.MAX_VALUE);
        for (int i = 0; i < ITERATIONS; i++) {
            A_INT.compareAndSet(this, i, i + 1);
        }
        if ((int) A_INT.getOpaque(this) != Integer.MAX_VALUE) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_Plain_WeakCompareAndSet_Success() {
        A_INT.setOpaque(this, 0);
        for (int i = 0; i < ITERATIONS; i++) {
            while (!A_INT.weakCompareAndSetPlain(this, i, i + 1) /* could fail spuriously */)
                ;
        }
        if ((int) A_INT.getOpaque(this) != ITERATIONS) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_Plain_WeakCompareAndSet_Failed() {
        A_INT.setOpaque(this, Integer.MAX_VALUE);
        for (int i = 0; i < ITERATIONS; i++) {
            A_INT.weakCompareAndSetPlain(this, i, i + 1);
        }
        if ((int) A_INT.getOpaque(this) != Integer.MAX_VALUE) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_GetVolatileSetPlain_CompareAndSet_Success() {
        A_INT.setOpaque(this, 0);
        for (int i = 0; i < ITERATIONS; i++) {
            while ((int) A_INT.getVolatile(this) == i) {
                if (A_INT.weakCompareAndSetPlain(this, i, i + 1)) {
                    break;
                }
            }
        }
        if ((int) A_INT.getOpaque(this) != ITERATIONS) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_GetVolatileSetPlain_CompareAndSet_Failed() {
        A_INT.setOpaque(this, Integer.MAX_VALUE);
        for (int i = 0; i < ITERATIONS; i++) {
            while ((int) A_INT.getVolatile(this) == i) {
                if (A_INT.weakCompareAndSetPlain(this, i, i + 1)) {
                    break;
                }
            }
        }
        if ((int) A_INT.getOpaque(this) != Integer.MAX_VALUE) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_GetVolatileSetRelease_CompareAndSet_Success() {
        A_INT.setOpaque(this, 0);
        for (int i = 0; i < ITERATIONS; i++) {
            while ((int) A_INT.getVolatile(this) == i) {
                if (A_INT.weakCompareAndSetRelease(this, i, i + 1)) {
                    break;
                }
            }
        }
        if ((int) A_INT.getOpaque(this) != ITERATIONS) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_GetVolatileSetRelease_CompareAndSet_Failed() {
        A_INT.setOpaque(this, Integer.MAX_VALUE);
        for (int i = 0; i < ITERATIONS; i++) {
            while ((int) A_INT.getVolatile(this) == i) {
                if (A_INT.weakCompareAndSetRelease(this, i, i + 1)) {
                    break;
                }
            }
        }
        if ((int) A_INT.getOpaque(this) != Integer.MAX_VALUE) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_GetVolatileSetVolatile_WeakCompareAndSet_Success() {
        A_INT.setOpaque(this, 0);
        for (int i = 0; i < ITERATIONS; i++) {
            while ((int) A_INT.getVolatile(this) == i) {
                if (A_INT.weakCompareAndSet(this, i, i + 1)) {
                    break;
                }
            }
        }
        if ((int) A_INT.getOpaque(this) != ITERATIONS) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testInt_GetVolatileSetVolatile_WeakCompareAndSet_Failed() {
        A_INT.setOpaque(this, Integer.MAX_VALUE);
        for (int i = 0; i < ITERATIONS; i++) {
            while ((int) A_INT.getVolatile(this) == i) {
                if (A_INT.weakCompareAndSet(this, i, i + 1)) {
                    break;
                }
            }
        }
        if ((int) A_INT.getOpaque(this) != Integer.MAX_VALUE) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testAtomicBoolean_Volatile_CompareAndSet_Failed() {
        anAtomicBoolean = new AtomicBoolean();
        anAtomicBoolean.set(true);
        for (int i = 0; i < ITERATIONS; i++) {
            anAtomicBoolean.compareAndSet(false, true);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testAtomicBoolean_NewFalse() {
        for (int i = 0; i < ITERATIONS; i++) {
            AN_ATOMIC_BOOLEAN.setOpaque(this, new AtomicBoolean(false));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testAtomicBoolean_NewDefault() {
        for (int i = 0; i < ITERATIONS; i++) {
            AN_ATOMIC_BOOLEAN.setOpaque(this, new AtomicBoolean());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public void testAtomicBoolean_NewTrue() {
        for (int i = 0; i < ITERATIONS; i++) {
            AN_ATOMIC_BOOLEAN.setOpaque(this, new AtomicBoolean(true));
        }
    }

}
