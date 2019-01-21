package hu.dbx.kompot.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * Used to manage lifecycle of a component. Some methods can be called only after the component has been successfully
 * started and some others are callable only before a component start is initiated.
 */
public final class BlockingLifecycle {

    private final ReadWriteLock stopping = new ReentrantReadWriteLock();

    private AtomicBoolean startInitiated = new AtomicBoolean(false);
    private AtomicBoolean startFinished = new AtomicBoolean(false);

    private AtomicBoolean stopInitiated = new AtomicBoolean(false);
    private AtomicBoolean stopFinished = new AtomicBoolean(false);


    public void doWhenRunning(Runnable r) {
        try {
            doWhenRunningGet(() -> {
                r.run();
                return null;
            });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T doWhenRunningGet(Supplier<T> r) {
        try {
            return doWhenRunning(r::get);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Exception> void doWhenRunningThrows(BodyThrows<T> r) throws T {
        try {
            doWhenRunning(() -> {
                r.run();
                return null;
            });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw (T) e;
        }
    }

    @SuppressWarnings("unchecked")
    public <R, T extends Exception> R doWhenRunningThrows(BodyThrowsReturns<R, T> r) throws T {
        try {
            return doWhenRunning(r::run);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw (T) e;
        }
    }


    @FunctionalInterface
    public interface BodyThrows<E extends Exception> {
        void run() throws E;
    }

    @FunctionalInterface
    public interface BodyThrowsReturns<R, E extends Exception> {
        R run() throws E;
    }

    @SuppressWarnings("UnusedReturnValue")
    public <T> T doWhenRunning(Callable<T> r) throws Exception {
        stopping.readLock().lock();
        try {
            if (!startFinished.get()) {
                throw new IllegalStateException("Can not execute becase state is not started yet!");
            } else if (stopInitiated.get()) {
                throw new IllegalStateException("Can not execute because state shutdown is already initiated!");
            } else {
                return r.call();
            }
        } finally {
            stopping.readLock().unlock();
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    public <T> T doBeforeStarted(Callable<T> r) throws Exception {
        stopping.readLock().lock();
        try {
            if (stopInitiated.get()) {
                throw new RuntimeException("Stop has already been initiated.");
            } else if (startInitiated.get()) {
                throw new IllegalStateException("Can run only before state has been started!");
            } else {
                return r.call();
            }
        } finally {
            stopping.readLock().unlock();
        }
    }

    /**
     * Evaluates body iff state has not been started yet.
     *
     * @throws NullPointerException  when runnable is null.
     * @throws IllegalStateException when state start has been requested.
     */
    public void doBeforeStarted(Runnable r) throws IllegalStateException {
        try {
            doBeforeStarted(() -> {
                r.run();
                return null;
            });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    public <T> T starting(Callable<T> r) throws IllegalStateException {
        if (!startInitiated.compareAndSet(false, true)) {
            throw new IllegalStateException("Component start has already beed initiated!");
        } else {
            final T t;
            try {
                t = r.call();
            } catch (Exception e) {
                throw new IllegalStateException("Exception when initializing", e);
            }
            startFinished.set(true);
            return t;
        }
    }

    public void starting(Runnable r) {
        try {
            starting(() -> {
                r.run();
                return null;
            });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void stopping(Runnable r) {
        if (!startFinished.get()) {
            throw new IllegalStateException("Can not stop component as it has not been started yet!");
        } else if (stopInitiated.get()) {
            throw new IllegalStateException("Component stop has already been requested before!");
        } else {
            stopping.writeLock().lock();
            try {
                if (!stopInitiated.compareAndSet(false, true)) {
                    throw new IllegalStateException("Can not stop component as stopping has already been requested!");
                } else {
                    r.run();
                    stopFinished.set(true);
                }
            } finally {
                stopping.writeLock().unlock();
            }
        }
    }

    public boolean isRunning() {
        return startFinished.get() && !stopInitiated.get();
    }
}
