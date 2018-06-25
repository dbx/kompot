package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventDescriptorResolver;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptorResolver;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.producer.ProducerIdentity;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.*;
import static java.lang.String.join;

public final class DataHandling {

    private static final Logger LOGGER = LoggerUtils.getLogger();


    public enum EventKeys {

        /**
         * Event code (name)
         */
        CODE,

        /**
         * Serialized event data
         */
        DATA,

        /**
         * Comma separated list of event names
         */
        GROUPS,

        /**
         * UUID of producer who initiated this event
         */
        SENDER,

        /**
         * Time when event was registered
         */
        FIRST_SENT,

        STATUS,

        ERROR_MSG
    }

    public enum Statuses {
        CREATED, ERROR, PROCESSED, PROCESSING
    }

    public enum MethodResponseKeys {
        RESPONSE, STATUS, EXCEPTION_CLASS, EXCEPTION_MESSAGE
    }

    public enum EventPriority {HI, LO}

    ;

    /**
     * Persists event details object.
     *
     * @param store          database connection
     * @param keyNaming      tells name to save under
     * @param groups         groups to save
     * @param eventFrame     event instance
     * @param clientIdentity the sender id
     * @throws SerializationException when can not serialize event data.
     */
    static void saveEventDetails(Transaction store, KeyNaming keyNaming, Iterable<String> groups, EventFrame eventFrame, ProducerIdentity clientIdentity) throws SerializationException {
        final String eventDetailsKey = keyNaming.eventDetailsKey(eventFrame.getIdentifier());

        LOGGER.debug("Saving event details under key {}", eventDetailsKey);

        store.hsetnx(eventDetailsKey, CODE.name(), eventFrame.getEventMarker().getEventName());
        store.hsetnx(eventDetailsKey, DATA.name(), SerializeHelper.serializeDataOnly(eventFrame));
        // TODO: use server time instead
        // store.hsetnx(eventDetailsKey, FIRST_SENT.name(), String.valueOf(store.time().get().get(0)));
        store.hsetnx(eventDetailsKey, FIRST_SENT.name(), String.valueOf(LocalDateTime.now()));
        store.hsetnx(eventDetailsKey, GROUPS.name(), join(",", groups));
        store.hsetnx(eventDetailsKey, SENDER.name(), clientIdentity.getIdentifier());

        LOGGER.debug("Saved event details key.");
    }

    /*
    public static Map<String, Object> loadDetails(SafeKVStoreFacade<String, String> store, KeyNaming naming, String uuid) {
        final Map<String, Object> m = new HashMap<>();
        m.put("uuid", uuid);
        m.put(CODE.name(), store.readMap(naming.eventDetailsKey(uuid), CODE.name()));
        m.put(FIRST_SENT.name(), store.readMap(naming.eventDetailsKey(uuid), FIRST_SENT.name()));
        m.put(DATA.name(), store.readMap(naming.eventDetailsKey(uuid), DATA.name()));
        m.put(GROUPS.name(), Arrays.asList(store.readMap(naming.eventDetailsKey(uuid), GROUPS.name()).split(","))); // all modules it was sent to.
        m.put(SENDER.name(), store.readMap(naming.eventDetailsKey(uuid), SENDER.name()));
        return m;
    }
    */

    /**
     * Egy esemenyt rateszt a feldolgozando sorra es a history-ba is elmenti.
     * Lementi az event group-ot is egy setbe.
     *
     * @param tx          jedis MULTI transaction instance
     * @param keyNaming   megmondja, h milyen kulcs ala tesszuk az esemenyt
     * @param eventId     melyik esemenyt akarjuk kikuldeni?
     * @param eventGroups a csoportok, akik ala bekerul az esemeny
     */
    static void saveEventGroups(Transaction tx, KeyNaming keyNaming, UUID eventId, Iterable<String> eventGroups) {
        for (String group : eventGroups) {
            zaddNow(tx, keyNaming.unprocessedEventsByGroupKey(group), eventId.toString());
            tx.hset(keyNaming.eventDetailsKey(group, eventId), STATUS.name(), Statuses.CREATED.name());

            tx.sadd(keyNaming.eventGroupsKey(), group);
        }
    }

    public static void zaddNow(Transaction tx, String k, String value) {
        long now = System.currentTimeMillis();
        double weight = ((double) now / Math.log(now)) + 3;
        tx.zadd(k, weight, value);
    }


    @SuppressWarnings("unchecked")
    static EventFrame readEventFrame(Jedis jedis, KeyNaming keyNaming, EventDescriptorResolver eventResolver, UUID eventUuid) throws DeserializationException {
        final String eventDetailsKey = keyNaming.eventDetailsKey(eventUuid);

        LOGGER.debug("Loading event details under key {}", eventDetailsKey);

        final String eventName = jedis.hget(eventDetailsKey, CODE.name());
        final String eventData = jedis.hget(eventDetailsKey, DATA.name());

        final Object eventDataObj = SerializeHelper.deserializeContentOnly(eventName, eventData, eventResolver);
        final Optional<EventDescriptor> eventMarker = eventResolver.resolveMarker(eventName);

        EventFrame response = new EventFrame();
        response.setEventMarker(eventMarker.get());
        response.setEventData(eventDataObj);
        response.setIdentifier(eventUuid);
        response.setSourceIdentifier(null); // did not set now.
        return response;
    }

    static void writeMethodFrame(Jedis jedis, KeyNaming keyNaming, MethodRequestFrame frame) throws SerializationException {
        final String methodDetailsKey = keyNaming.methodDetailsKey(frame.getIdentifier());
        // minel elobb dobjunk kivetelt!
        final String serialized = SerializeHelper.serializeObject(frame.getMethodData());

        LOGGER.debug("Writing method frame to key {}", methodDetailsKey);

        // TODO: legyen multi/exec!
        jedis.hset(methodDetailsKey, CODE.name(), frame.getMethodMarker().getMethodName());
        jedis.hset(methodDetailsKey, SENDER.name(), frame.getSourceIdentifier());
        jedis.hset(methodDetailsKey, DATA.name(), serialized);
        final int expiration = ((int) Math.ceil(((double) frame.getMethodMarker().getTimeout()) / 1000));
        LOGGER.debug("Setting expiration to {} seconds on key {}", expiration, methodDetailsKey);

        jedis.expire(methodDetailsKey, expiration);
    }

    @SuppressWarnings("unchecked")
    static Optional<MethodRequestFrame> readMethodFrame(Jedis jedis, KeyNaming keyNaming, MethodDescriptorResolver resolver, UUID methodUuid) throws DeserializationException {
        final String methodDetailsKey = keyNaming.methodDetailsKey(methodUuid);
        final String methodName = jedis.hget(methodDetailsKey, CODE.name());
        if (methodName == null)
            return Optional.empty();

        final String methodData = jedis.hget(methodDetailsKey, DATA.name());
        final String sender = jedis.hget(methodDetailsKey, SENDER.name());

        final Optional<MethodDescriptor> descriptor = resolver.resolveMarker(methodName);
        if (!descriptor.isPresent()) return Optional.empty();

        final Object requestData = SerializeHelper.deserializeRequest(methodName, methodData, resolver);

        final MethodRequestFrame frame = MethodRequestFrame.build(methodUuid, ProducerIdentity.constantly(sender), descriptor.get(), requestData);
        return Optional.of(frame);
    }

}
