package hu.dbx.kompot.impl.jedis.status;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.impl.jedis.JedisMessagingService;
import hu.dbx.kompot.status.AbstractStatusReportingAction;
import hu.dbx.kompot.status.StatusReport;
import hu.dbx.kompot.status.StatusRequestBroadcastHandler;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;

public class JedisStatusReportingAction extends AbstractStatusReportingAction {

    public JedisStatusReportingAction(ConsumerImpl consumer, CommunicationEndpoint endpoint) {
        super(consumer, endpoint);
    }

    @Override
    protected StatusRequestBroadcastHandler getStatusRequestBroadcastHandler() {
        return new JedisStatusRequestBroadcastHandler(this::findLocalStatusReport, consumer.getConsumerConfig());
    }

    @Override
    public List<StatusReport> findGlobalStatuses() {

        // wait for one module.
        final int MODULE_TIMEOUT_SECS = 20;

        final JedisMessagingService messagingService = (JedisMessagingService) consumer.getConsumerConfig().getMessagingService();

        try (Jedis jedis = messagingService.getPool().getResource()) {
            final Set<String> ks = SelfStatusWriter.findStatusKeys(jedis, messagingService.getKeyNaming());
            final int moduleCount = ks.size(); // ennyi darab modul fut osszesen

            // some random key where i expect the response.
            final String newKey = messagingService.getKeyNaming().statusResponseKey();

            // send broadcast
            final Map<String, String> broadcastData = singletonMap("key", newKey);
            endpoint.broadcast(StatusRequestBroadcastHandler.DESCRIPTOR, broadcastData);

            // wait for responses from all modules.
            final List<StatusReport> reports = new ArrayList<>(moduleCount);
            for (int i = 0; i < moduleCount; i++) {
                List<String> popped = jedis.blpop(MODULE_TIMEOUT_SECS, newKey);
                if (popped != null) {
                    final String response = popped.get(1);
                    reports.add(SerializeHelper.deserializeStatus(response));
                } else {
                    // timeout happened
                    break;
                }
            }
            return reports;

        } catch (SerializationException | DeserializationException e) {
            throw new RuntimeException(e);
        }
    }

}
