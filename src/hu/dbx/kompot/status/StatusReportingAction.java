package hu.dbx.kompot.status;


import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.impl.LoggerUtils;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;

public class StatusReportingAction {

    private static final Logger LOGGER = LoggerUtils.getLogger();


    private final List<StatusReporter> statusReporters = new ArrayList<>();

    private final ConsumerImpl consumer;
    private final CommunicationEndpoint endpoint;

    public StatusReportingAction(ConsumerImpl consumer, CommunicationEndpoint endpoint) {
        this.consumer = consumer;
        this.endpoint = endpoint;
        endpoint.registerBroadcastProcessor(new StatusRequestBroadcastHandler(this::findLocalStatusReport, consumer.getConsumerConfig()));
    }

    public void registerStatusReporter(StatusReporter reporter) {
        statusReporters.add(reporter);
    }


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
                statusResult = StatusReporter.StatusResult.resultError(e.getMessage());
            }
            result.add(new StatusItemImpl(statusReporter.getName(), statusReporter.getDescription(), statusResult.getErrorMessage()));
        }
        return result;
    }

    private StatusReport findLocalStatusReport() {
        final List<StatusReport.StatusItem> items = findLocalStatuses();
        final ConsumerIdentity id = consumer.getConsumerIdentity();

        // TODO: collect this info.

        final Set<String> methods = emptySet();
        final Set<String> events = emptySet();
        final Set<String> broadcasts = consumer.getBroadcastProcessorAdapter().getSupportedBroadcasts().stream().map(BroadcastDescriptor::getBroadcastCode).collect(toSet());

        return new StatusReport(id, null, items, methods, events, broadcasts);
    }

    public List<StatusReport> findGlobalStatuses() {

        try (Jedis jedis = consumer.getConsumerConfig().getPool().getResource()) {
            final Set<String> ks = SelfStatusWriter.findStatusKeys(jedis, consumer.getKeyNaming());
            final int moduleCount = ks.size(); // ennyi darab modul fut osszesen

            // some random key where i expect the response.
            final String newKey = consumer.getKeyNaming().statusResponseKey();

            // send broadcast
            final Map<String, String> broadcastData = singletonMap("key", newKey);
            endpoint.broadcast(StatusRequestBroadcastHandler.DESCRIPTOR, broadcastData);

            // wait for responses from all modules.
            final List<StatusReport> reports = new ArrayList<>(moduleCount);
            for (int i = 0; i < moduleCount; i++) {
                final String response = jedis.blpop(newKey, "5").get(1); // TODO: wait with timeout here.
                reports.add(SerializeHelper.deserializeStatus(response));
            }
            return reports;

        } catch (SerializationException | DeserializationException e) {
            throw new RuntimeException(e);
        }
    }
}
