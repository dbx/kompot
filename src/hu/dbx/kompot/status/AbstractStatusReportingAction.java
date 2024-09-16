package hu.dbx.kompot.status;


import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.core.StatusReportingAction;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.impl.LoggerUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public abstract class AbstractStatusReportingAction implements StatusReportingAction {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final List<StatusReporter> statusReporters = new ArrayList<>();

    protected final ConsumerImpl consumer;
    protected final CommunicationEndpoint endpoint;

    public AbstractStatusReportingAction(ConsumerImpl consumer, CommunicationEndpoint endpoint) {
        this.consumer = consumer;
        this.endpoint = endpoint;
        endpoint.registerBroadcastProcessor(getStatusRequestBroadcastHandler());
    }

    @Override
    public void registerStatusReporter(StatusReporter reporter) {
        statusReporters.add(reporter);
    }

    protected abstract StatusRequestBroadcastHandler getStatusRequestBroadcastHandler();

    /**
     * Returns a list of all systems found in the current component.
     */
    private List<StatusReport.StatusItem> findLocalStatuses() {

        List<StatusReport.StatusItem> result = new ArrayList<>();

        for (StatusReporter statusReporter : statusReporters) {
            StatusReporter.StatusResult statusResult;
            try {
                statusResult = statusReporter.getEndpoint().call();
            } catch (Exception e) {
                LOGGER.error("Error caught while calling statusReporter " + statusReporter, e);
                statusResult = StatusReporter.StatusResult.error(e.getMessage());
            }
            result.add(new StatusItemImpl(statusReporter.getName(), statusReporter.getDescription(), statusResult.getStatusMessage(), statusResult.getErrorMessage()));
        }
        return result;
    }

    protected StatusReport findLocalStatusReport() {
        final List<StatusReport.StatusItem> items = findLocalStatuses();
        final ConsumerIdentity id = consumer.getConsumerIdentity();

        // TODO: collect this info.

        final Set<String> methods = emptySet();
        final Set<String> events = emptySet();
        final Set<String> broadcasts = consumer.getBroadcastProcessorAdapter().getSupportedBroadcasts().stream().map(BroadcastDescriptor::getBroadcastCode).collect(toSet());

        return new StatusReport(id, new Date(), items, methods, events, broadcasts);
    }

    public abstract List<StatusReport> findGlobalStatuses();

}
