package com.bigdata.it4931.utility.concurrent;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
public class BatchProcessor<K, V> implements Closeable {

    private final AtomicBoolean isDisposed = new AtomicBoolean(false);
    private final ExecutorService executorService;
    private final BlockingQueue<Map.Entry<K, AsyncCallback<V>>> queue;
    private final Semaphore semaphore;
    private final int corePoolSize;
    private final int batchSize;
    private final boolean isMustFullBatch;
    private final Consumer<List<Map.Entry<K, AsyncCallback<V>>>> applyFn;

    private BatchProcessor(Builder<K, V> builder, Consumer<List<Map.Entry<K, AsyncCallback<V>>>> applyFn) {
        this.executorService = builder.executorService;
        this.corePoolSize = builder.corePoolSize;
        this.queue = builder.queue;
        this.batchSize = builder.batchSize;
        this.isMustFullBatch = builder.isMustFullBatch;

        this.semaphore = new Semaphore(this.corePoolSize);
        this.applyFn = applyFn;

        for (int i = 0; i < corePoolSize; i++) {
            CompletableFuture.runAsync(this::processBatch, executorService);
        }
    }

    private void processBatch() {
        while (!isDisposed.get()) {
            List<Map.Entry<K, AsyncCallback<V>>> batch = new LinkedList<>();
            try {
                if (isMustFullBatch) {
                    waitForFullBatch(batch);
                } else {
                    int drained = queue.drainTo(batch, batchSize);
                    if (drained == 0) {
                        semaphore.acquire();
                        continue;
                    }
                }
                if (!batch.isEmpty()) {
                    applyFn.accept(batch);
                }
            } catch (Exception e) {
                handleBatchException(batch, e);
            } finally {
                completeBatch(batch);
            }
        }
    }

    private void waitForFullBatch(List<Map.Entry<K, AsyncCallback<V>>> batch) throws InterruptedException {
        synchronized (queue) {
            while (queue.size() < batchSize && !isDisposed.get()) {
                Threads.sleep(500);
            }
            queue.drainTo(batch, batchSize);
        }
    }

    private void handleBatchException(List<Map.Entry<K, AsyncCallback<V>>> batch, Exception e) {
        for (Map.Entry<K, AsyncCallback<V>> entry : batch) {
            entry.getValue().completeExceptionally(e);
        }
    }

    private void completeBatch(List<Map.Entry<K, AsyncCallback<V>>> batch) {
        for (Map.Entry<K, AsyncCallback<V>> entry : batch) {
            entry.getValue().complete();
        }
    }

    public static <K, V> Builder<K, V> newBuilder() {
        return new Builder<>();
    }

    public AsyncCallback<V> put(K payload) {
        AsyncCallback<V> callback = new AsyncCallback<>();
        queue.add(new AbstractMap.SimpleImmutableEntry<>(payload, callback));
        semaphore.release();
        return callback;
    }

    @Override
    public void close() {
        while (!queue.isEmpty()) {
            processAll();
            Threads.sleep(500);
        }
        isDisposed.set(true);
        executorService.shutdown();
    }

    public void processAll() {
        List<Map.Entry<K, AsyncCallback<V>>> batch = new LinkedList<>();
        queue.drainTo(batch, queue.size());
        applyFn.accept(batch);
    }

    public static class Builder<K, V> {
        public ExecutorService executorService = Executors.newCachedThreadPool();
        public int corePoolSize = 1;
        public BlockingQueue<Map.Entry<K, AsyncCallback<V>>> queue = new LinkedBlockingQueue<>();
        public int batchSize = 100;
        public boolean isMustFullBatch = false;

        public Builder<K, V> with(Consumer<Builder<K, V>> consumer) {
            consumer.accept(this);
            return this;
        }

        public BatchProcessor<K, V> build(Consumer<List<Map.Entry<K, AsyncCallback<V>>>> applyFn) {
            return new BatchProcessor<>(this, applyFn);
        }
    }
}
