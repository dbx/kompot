package hu.dbx.kompot.ng.methods;

import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.consumer.sync.MethodSendingCallback;

import java.util.*;

import static hu.dbx.kompot.ng.methods.MethodLifecycleTester.Events.*;
import static org.junit.Assert.assertEquals;

final class MethodLifecycleTester implements MethodSendingCallback {

    enum Events {SENT, RECEIVED, ERROR, TIMEOUT}

    private final Map<UUID, List<Events>> frameEvents = Collections.synchronizedMap(new HashMap<>());

    void assertSentAndReceived() {
        frameEvents.values().forEach(v -> assertEquals(Arrays.asList(SENT, RECEIVED), v));
    }

    @Override
    public void onRequestSent(MethodRequestFrame frame) {
        frameEvents.computeIfAbsent(frame.getIdentifier(), __ -> new LinkedList<>()).add(SENT);
    }

    @Override
    public void onResponseReceived(MethodRequestFrame frame, Object response) {
        frameEvents.computeIfAbsent(frame.getIdentifier(), __ -> new LinkedList<>()).add(RECEIVED);
    }

    @Override
    public void onErrorReceived(MethodRequestFrame frame, Throwable t) {
        frameEvents.computeIfAbsent(frame.getIdentifier(), __ -> new LinkedList<>()).add(ERROR);
    }

    @Override
    public void onTimeOut(MethodRequestFrame frame) {
        frameEvents.computeIfAbsent(frame.getIdentifier(), __ -> new LinkedList<>()).add(TIMEOUT);
    }
}
