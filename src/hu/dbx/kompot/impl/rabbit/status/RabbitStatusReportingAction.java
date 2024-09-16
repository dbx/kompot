package hu.dbx.kompot.impl.rabbit.status;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.status.AbstractStatusReportingAction;
import hu.dbx.kompot.status.StatusReport;
import hu.dbx.kompot.status.StatusRequestBroadcastHandler;

import java.util.*;

public class RabbitStatusReportingAction extends AbstractStatusReportingAction {

    private final int TWENTY_SECONDS = 20 * 1000;

    private final Map<UUID, StatusReport> statusReports = new HashMap<>();

    public RabbitStatusReportingAction(ConsumerImpl consumer, CommunicationEndpoint endpoint) {
        super(consumer, endpoint);
        endpoint.registerBroadcastProcessor(new RabbitStatusResponseBroadcastHandler(this));
    }

    public void addStatusReport(final StatusReport statusReport) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, -1);
        statusReports.entrySet().removeIf(it -> it.getValue().getLastHeartbeatTime().before(cal.getTime()));
        statusReports.put(statusReport.getModuleIdentifier(), statusReport);
    }

    @Override
    protected StatusRequestBroadcastHandler getStatusRequestBroadcastHandler() {
        return new RabbitStatusRequestBroadcastHandler(this::findLocalStatusReport, consumer.getConsumerConfig());
    }

    @Override
    public List<StatusReport> findGlobalStatuses() {
        try {
            endpoint.broadcast(StatusRequestBroadcastHandler.DESCRIPTOR, Collections.emptyMap());
            Thread.sleep(TWENTY_SECONDS);
        } catch (SerializationException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return new ArrayList<>(statusReports.values());
    }

}
