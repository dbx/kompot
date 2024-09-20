package hu.dbx.kompot.impl.jedis;

import hu.dbx.kompot.consumer.async.EventDescriptorResolver;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.sync.MethodDescriptorResolver;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.events.Priority;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.exceptions.MessageErrorResultException;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.moby.MetaDataHolder;
import hu.dbx.kompot.producer.ProducerIdentity;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.StreamSupport;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.*;
import static hu.dbx.kompot.impl.DataHandling.MethodResponseKeys.EXCEPTION_CLASS;
import static hu.dbx.kompot.impl.DataHandling.MethodResponseKeys.EXCEPTION_MESSAGE;
import static java.lang.String.join;

public final class JedisHelper {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    public static final String GROUP_SEPARATOR_CHAR = ",";

    private JedisHelper() {
    }

    public static MessageErrorResultException deserializeException(String methodDetailsKey, Jedis connection) {
        final String className = connection.hget(methodDetailsKey, EXCEPTION_CLASS.name());
        final String message = connection.hget(methodDetailsKey, EXCEPTION_MESSAGE.name());

        return new MessageErrorResultException(message, className);
    }

    public static void writeMethodFrame(Transaction jedis, KeyNaming keyNaming, MethodRequestFrame<?> frame) {
        final String methodDetailsKey = keyNaming.methodDetailsKey(frame.getIdentifier());
        // minel elobb dobjunk kivetelt!
//        final String serialized = SerializeHelper.serializeObject(frame.getMethodData());

        LOGGER.debug("Writing method frame to key {}", methodDetailsKey);

        // TODO: legyen multi/exec!
        jedis.hset(methodDetailsKey, CODE.name(), frame.getMethodMarker().getMethodName());
        jedis.hset(methodDetailsKey, SENDER.name(), frame.getSourceIdentifier());
        jedis.hset(methodDetailsKey.getBytes(), DATA_ZIP.name().getBytes(), SerializeHelper.compressData(frame.getMethodData()));

        saveMetaData(jedis, frame.getMetaData(), methodDetailsKey);

        final int expiration = ((int) Math.ceil(((double) frame.getMethodMarker().getTimeout()) / 1000));
        LOGGER.debug("Setting expiration to {} seconds on key {}", expiration, methodDetailsKey);

        jedis.expire(methodDetailsKey, expiration);
    }


    @SuppressWarnings("unchecked")
    public static Optional<MethodRequestFrame<?>> readMethodFrame(Jedis jedis,
                                                               KeyNaming keyNaming,
                                                               MethodDescriptorResolver resolver,
                                                               UUID methodUuid,
                                                               List<String> logSensitiveDataKeys) throws DeserializationException {
        final String methodDetailsKey = keyNaming.methodDetailsKey(methodUuid);
        final String methodName = jedis.hget(methodDetailsKey, CODE.name());
        if (methodName == null) {
            LOGGER.warn("Missing method name under key {}", methodDetailsKey);
            return Optional.empty();
        }

        final String methodData = jedis.hget(methodDetailsKey, DATA.name());
        final byte[] methodDataZip = jedis.hget(methodDetailsKey.getBytes(), DATA_ZIP.name().getBytes());
        final String sender = jedis.hget(methodDetailsKey, SENDER.name());
        final MetaDataHolder methodMeta = readMetaData(jedis, methodDetailsKey);

        return DataHandling.readMethodFrame(methodName, methodData, methodDataZip, methodMeta, resolver, methodUuid, sender, logSensitiveDataKeys);
    }

    private static MetaDataHolder readMetaData(Jedis jedis, String detailsKey) {
        final String corrId = jedis.hget(detailsKey, MetaDataHolder.MetaDataFields.CORRELATION_ID.name());
        final String userRef = jedis.hget(detailsKey, MetaDataHolder.MetaDataFields.USER_REF.name());
        final String sourceName = jedis.hget(detailsKey, MetaDataHolder.MetaDataFields.SOURCE_NAME.name());
        final String batchIdStr = jedis.hget(detailsKey, MetaDataHolder.MetaDataFields.BATCH_ID.name());
        final String feedbackUuidStr = jedis.hget(detailsKey, MetaDataHolder.MetaDataFields.FEEDBACK_UUID.name());

        MetaDataHolder meta = MetaDataHolder.build(corrId, userRef, sourceName, null);

        if (batchIdStr != null && !batchIdStr.trim().isEmpty()) {
            meta = meta.withBatchId(Long.valueOf(batchIdStr));
        }

        if (feedbackUuidStr != null && !feedbackUuidStr.trim().isEmpty()) {
            meta = meta.withFeedbackUuid(UUID.fromString(feedbackUuidStr.trim()));
        }

        return meta;
    }

    public static void decrementUnprocessedGroupsCounter(Transaction store, KeyNaming keyNaming, UUID eventDataUuid) {
        final String eventDetailsKey = keyNaming.eventDetailsKey(eventDataUuid);

        store.hincrBy(eventDetailsKey, UNPROCESSED_GROUPS.name(), -1);
    }


    /**
     * Egy esemenyt rateszt a feldolgozando sorra es a history-ba is elmenti.
     * Lementi az event group-ot is egy setbe.
     *
     * @param tx          jedis MULTI transaction instance
     * @param keyNaming   megmondja, h milyen kulcs ala tesszuk az esemenyt
     * @param eventId     melyik esemenyt akarjuk kikuldeni?
     * @param eventGroups a csoportok, akik ala bekerul az esemeny
     */
    public static void saveEventGroups(Transaction tx, KeyNaming keyNaming, UUID eventId, Priority priority, Iterable<String> eventGroups) {
        for (String group : eventGroups) {
            zaddNow(tx, keyNaming.unprocessedEventsByGroupKey(group), priority, eventId.toString().getBytes());
            tx.hset(keyNaming.eventDetailsKey(group, eventId), STATUS.name(), DataHandling.Statuses.CREATED.name());

            tx.sadd(keyNaming.eventGroupsKey(), group);
        }
    }

    private static final long PRIORITY_LEVEL_WEIGHT_OFFSET = 1L << 35; //kb 400 nap (millisecben)

    public static void zaddNow(Transaction tx, String sortedSetKey, Priority priority, byte[] value) {
        long now = System.currentTimeMillis();

        double weight = now - priority.score * PRIORITY_LEVEL_WEIGHT_OFFSET;
        tx.zadd(sortedSetKey.getBytes(), weight, value);
    }

    /**
     * Reads event data from redis by uuid.
     *
     * @param jedis                db connection
     * @param keyNaming            used to get keys used in this redis instance.
     * @param eventResolver        used to find event descriptor
     * @param eventUuid            event identifier
     * @param logSensitiveDataKeys
     * @return read frame - not null
     * @throws DeserializationException on deserialization error
     * @throws IllegalStateException    when no event descriptor is found for evt type
     * @throws IllegalArgumentException could not find event data in redis
     */
    public static EventFrame<?> readEventFrame(Jedis jedis,
                                            KeyNaming keyNaming,
                                            EventDescriptorResolver eventResolver,
                                            UUID eventUuid,
                                            List<String> logSensitiveDataKeys) throws DeserializationException {
        final String eventDetailsKey = keyNaming.eventDetailsKey(eventUuid);

        LOGGER.debug("Loading event details under key {}", eventDetailsKey);

        final String eventName = jedis.hget(eventDetailsKey, CODE.name());
        final String eventSender = jedis.hget(eventDetailsKey, SENDER.name());

        final String eventData = jedis.hget(eventDetailsKey, DATA.name());
        final byte[] eventDataZip = jedis.hget(eventDetailsKey.getBytes(), DATA_ZIP.name().getBytes());
        final MetaDataHolder metaDataHolder = readMetaData(jedis, eventDetailsKey);

        return DataHandling.readEventFrame(eventName, eventData, eventDataZip, metaDataHolder, eventResolver, eventUuid, eventSender, logSensitiveDataKeys);
    }

    /**
     * Persists event details object.
     *
     * @param store          database connection
     * @param keyNaming      tells name to save under
     * @param groups         groups to save
     * @param eventFrame     event instance
     * @param clientIdentity the sender id
     */
    public static void saveEventDetails(Transaction store, KeyNaming keyNaming, Iterable<String> groups, EventFrame<?> eventFrame, ProducerIdentity clientIdentity) {
        final String eventDetailsKey = keyNaming.eventDetailsKey(eventFrame.getIdentifier());

        LOGGER.debug("Saving event details under key {}", eventDetailsKey);

        store.hsetnx(eventDetailsKey, CODE.name(), eventFrame.getEventMarker().getEventName());
        // TODO: use server time instead
        // store.hsetnx(eventDetailsKey, FIRST_SENT.name(), String.valueOf(store.time().get().get(0)));
        store.hsetnx(eventDetailsKey, FIRST_SENT.name(), String.valueOf(LocalDateTime.now()));
        store.hsetnx(eventDetailsKey, GROUPS.name(), formatGroupsString(groups));
        store.hsetnx(eventDetailsKey, SENDER.name(), clientIdentity.getIdentifier());
        store.hsetnx(eventDetailsKey, PRIORITY.name(), eventFrame.getEventMarker().getPriority().name());

        long groupCount = StreamSupport.stream(groups.spliterator(), false).count();
        store.hsetnx(eventDetailsKey, UNPROCESSED_GROUPS.name(), Long.toString(groupCount));

        saveMetaData(store, eventFrame.getMetaData(), eventDetailsKey);
        store.hsetnx(eventDetailsKey.getBytes(), DATA_ZIP.name().getBytes(), SerializeHelper.compressData(eventFrame.getEventData()));

        LOGGER.trace("Saved event details key.");
    }

    private static void saveMetaData(Transaction store, MetaDataHolder metaData, String detailsKey) {
        if (metaData != null) {
            if (metaData.getCorrelationId() != null) {
                store.hset(detailsKey, MetaDataHolder.MetaDataFields.CORRELATION_ID.name(), metaData.getCorrelationId());
            }
            if (metaData.getUserRef() != null) {
                store.hset(detailsKey, MetaDataHolder.MetaDataFields.USER_REF.name(), metaData.getUserRef());
            }
            if (metaData.getSourceName() != null) {
                store.hset(detailsKey, MetaDataHolder.MetaDataFields.SOURCE_NAME.name(), metaData.getSourceName());
            }
            if (metaData.getBatchId() != null) {
                store.hsetnx(detailsKey, MetaDataHolder.MetaDataFields.BATCH_ID.name(), metaData.getBatchId().toString());
            }
            if (metaData.getFeedbackUuid() != null) {
                store.hsetnx(detailsKey, MetaDataHolder.MetaDataFields.FEEDBACK_UUID.name(), metaData.getFeedbackUuid().toString());
            }
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static String formatGroupsString(Iterable<String> groups) {
        return join(GROUP_SEPARATOR_CHAR, groups);
    }

    public static Collection<String> parseGroupsString(String groups) {
        if (groups == null || groups.isEmpty()) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(groups.split(GROUP_SEPARATOR_CHAR));
        }
    }

}