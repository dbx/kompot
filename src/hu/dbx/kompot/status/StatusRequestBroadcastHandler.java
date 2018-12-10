package hu.dbx.kompot.status;

import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;

public class StatusRequestBroadcastHandler implements SelfDescribingBroadcastProcessor {

    @Override
    public BroadcastDescriptor getBroadcastDescriptor() {
        return null;
    }

    @Override
    public void handle(Object request) {

    }
}
