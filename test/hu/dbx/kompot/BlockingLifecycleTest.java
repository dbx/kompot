package hu.dbx.kompot;

import hu.dbx.kompot.impl.BlockingLifecycle;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class BlockingLifecycleTest {

    @Test
    public void test1() {
        // GIVEN
        final BlockingLifecycle lifecycle = new BlockingLifecycle();
        final AtomicInteger ai = new AtomicInteger(0);

        // WHEN
        lifecycle.starting(() -> ai.addAndGet(1));
        lifecycle.doWhenRunning((Runnable) () -> ai.addAndGet(2));
        lifecycle.stopping(() -> ai.addAndGet(4));

        // THEN
        assertEquals(7, ai.get());
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotJustStop() {
        // GIVEN
        final BlockingLifecycle lifecycle = new BlockingLifecycle();

        // WHEN
        lifecycle.stopping(this::toString);

        // THEN exception
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotStartTwice() {
        // GIVEN
        final BlockingLifecycle lifecycle = new BlockingLifecycle();

        // WHEN
        lifecycle.starting(this::toString);
        lifecycle.starting(this::toString);
        // THEN exception
    }


    @Test(expected = IllegalStateException.class)
    public void canNotExecBeforeStarted() throws Exception {
        // GIVEN
        final BlockingLifecycle lifecycle = new BlockingLifecycle();

        // WHEN
        lifecycle.doWhenRunning(this::toString);
    }

    @Test()
    public void testDoBeforeStartedSucceeds() throws Exception {
        // GIVEN
        final BlockingLifecycle lifecycle = new BlockingLifecycle();

        // WHEN
        lifecycle.doBeforeStarted(this::toString);
    }

    @Test(expected = IllegalStateException.class)
    public void testDoBeforeStartedFails() throws Exception {
        // GIVEN
        final BlockingLifecycle lifecycle = new BlockingLifecycle();

        lifecycle.starting(this::toString);

        // WHEN
        lifecycle.doBeforeStarted(this::toString);
    }
}
