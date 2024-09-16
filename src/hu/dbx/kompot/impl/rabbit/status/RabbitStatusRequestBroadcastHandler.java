package hu.dbx.kompot.impl.rabbit.status;

import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.status.StatusReport;
import hu.dbx.kompot.status.StatusRequestBroadcastHandler;

import java.util.Map;
import java.util.function.Supplier;

public class RabbitStatusRequestBroadcastHandler extends StatusRequestBroadcastHandler {

    public RabbitStatusRequestBroadcastHandler(Supplier<StatusReport> statusReportFactory, ConsumerConfig config) {
        super(statusReportFactory, config);
    }

    @Override
    public void handle(Map request) {
        try {
            final StatusReport status = statusReportFactory.get();
            LOGGER.info("Current status is: {}", status);
            final String serialized = SerializeHelper.serializeObject(status);
            config.getMessagingService().broadcast(RabbitStatusResponseBroadcastHandler.DESCRIPTOR, serialized);
        } catch (SerializationException e) {
            throw new RuntimeException(e);
        }
    }

}
