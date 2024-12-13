package com.bigdata.it4931.utility.concurrent;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncCallback<T> {

    private final Semaphore lock = new Semaphore(0);
    private final AtomicBoolean done = new AtomicBoolean(false);
    private T result = null;
    private Exception exception = null;

    public T get() throws Exception {
        lock.acquire();
        try {
            if (exception != null) {
                throw exception;
            }
            return result;
        } finally {
            lock.release();
        }
    }

    public void join() throws Exception {
        get();
    }

    public void complete() {
        complete(null, null);
    }

    public void complete(T result) {
        complete(null, result);
    }

    public void completeExceptionally(Exception e) {
        complete(e, null);
    }

    private void complete(Exception e, T result) {
        if (done.compareAndSet(false, true)) {
            this.exception = e;
            this.result = result;
            lock.release();
        }
    }
}
