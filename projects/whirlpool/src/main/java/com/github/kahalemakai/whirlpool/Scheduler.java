package com.github.kahalemakai.whirlpool;

import lombok.val;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Scheduler {

    private final ScheduledExecutorService scheduler;

    private Scheduler(int parallelism) {
        this.scheduler = Executors.newScheduledThreadPool(parallelism, WhirlpoolThreadFactory.getInstance());
    }

    public static Scheduler ofThreads(int parallelism) {
        if (parallelism == 0) {
            return new NoOpScheduler();
        }
        return new Scheduler(parallelism);
    }

    public ScheduledFuture<?> scheduleEviction(PoolEntry<?> entry, long delay) {
        val task = EvictEntryTask.of(entry);
        return scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    public <T> Future<T> createElement(CreateElementTask<T> task) {
        return scheduler.submit(task::createElement);
    }

    public Future<?> submit(Runnable r) {
        return scheduler.submit(r);
    }

    public void repeatedly(Runnable r, long intervalInMilliSeconds) {
        scheduler.scheduleAtFixedRate(r, intervalInMilliSeconds, intervalInMilliSeconds, TimeUnit.MILLISECONDS);
    }

    public Future<?> closeElement(CloseElementTask task) {
        return scheduler.submit(task);
    }

    public <T> List<Future<T>> createElements(CreateElementTask<T> task, int count) {
        return Stream.iterate(task, t -> t)
                .map(this::createElement)
                .limit(count)
                .collect(Collectors.toList());


    }

    private static class WhirlpoolThreadFactory implements ThreadFactory {
        public static final WhirlpoolThreadFactory INSTANCE = new WhirlpoolThreadFactory();

        private final ThreadGroup parentGroup;
        private final ThreadGroup evictionGroup;
        private final ThreadGroup genericGroup;
        private final AtomicInteger evictionThreadCounter = new AtomicInteger(0);
        private final AtomicInteger genericThreadCounter = new AtomicInteger(0);

        private WhirlpoolThreadFactory() {
            this.parentGroup = new ThreadGroup("whirlpool");
            this.genericGroup = new ThreadGroup(parentGroup, "bg");
            val evictionGroup = new ThreadGroup(parentGroup, "eviction");
            val priority = Math.min(Thread.currentThread().getPriority(), 3);
            evictionGroup.setMaxPriority(priority);
            this.evictionGroup = evictionGroup;
        }

        public static WhirlpoolThreadFactory getInstance() {
            return INSTANCE;
        }

        public Thread newThread(Runnable r) {
            return ((r instanceof EvictEntryTask) || (r instanceof CloseElementTask)
                    ? newEvictionThread(r)
                    : newCreationThread(r));
        }

        private Thread newEvictionThread(Runnable task) {
            Thread t = new Thread(
                    evictionGroup,
                    task,
                    String.format("%s-eviction-%d",
                            parentGroup.getName(), evictionThreadCounter.getAndIncrement()),
                    0);
            t.setPriority(evictionGroup.getMaxPriority());
            t.setDaemon(true);
            return t;
        }

        private Thread newCreationThread(Runnable task) {
            val thread = new Thread(
                    genericGroup,
                    task,
                    String.format("%s-generic-%d",
                            parentGroup.getName(), genericThreadCounter.getAndIncrement()),
                    0);
            thread.setDaemon(false);
            return thread;
        }

    }

    private static class EvictEntryTask extends TimerTask {
        private final WeakReference<PoolEntry<?>> entryRef;

        private EvictEntryTask(PoolEntry<?> entry) {
            this.entryRef = new WeakReference<>(entry);
        }

        @Override
        public void run() {
            val entry = entryRef.get();
            if (entry == null || !entry.tryMarkAsUsed()) {
                return;
            }
            entry.evict();
        }

        public static EvictEntryTask of(PoolEntry<?> entry) {
            return new EvictEntryTask(entry);
        }

    }

    private static class NoOpScheduler extends Scheduler {
        public NoOpScheduler() {
            super(0);
        }

        @Override
        public ScheduledFuture<?> scheduleEviction(PoolEntry<?> entry, long delay) {
            return (ScheduledNoOpFuture<?>) () -> null;
        }

        @Override
        public <T> Future<T> createElement(CreateElementTask<T> task) {
            return (NoOpFuture<T>) task::createElement;
        }

        @Override
        public Future<?> submit(Runnable r) {
            return (NoOpFuture<?>) () -> {
                r.run();
                return null;
            };
        }

        @Override
        public void repeatedly(Runnable r, long intervalInMilliSeconds) {
            throw new UnsupportedOperationException("cannot schedule job repeatedly on a single thread");
        }

        @Override
        public Future<?> closeElement(CloseElementTask task) {
            return (NoOpFuture<?>) () -> {
                task.run();
                return null;
            };
        }

        @Override
        public <T> List<Future<T>> createElements(CreateElementTask<T> task, int count) {
            return Stream.iterate(task, t -> t)
                    .map(t -> (NoOpFuture<T>) t::createElement)
                    .limit(count)
                    .collect(Collectors.toList());
        }
    }

    @FunctionalInterface
    private interface NoOpFuture<T> extends Future<T> {
        @Override
        default boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        default boolean isCancelled() {
            return false;
        }

        @Override
        default boolean isDone() {
            return true;
        }

        @Override
        default T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return get();
        }
    }

    @FunctionalInterface
    private interface ScheduledNoOpFuture<T> extends NoOpFuture<T>, ScheduledFuture<T> {
        @Override
        default long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        default int compareTo(Delayed o) {
            return 0;
        }
    }


}
