package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodReceivingCallback;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static hu.dbx.kompot.impl.LoggerUtils.debugMethodFrame;

/**
 * Represents a job for handling a method request.
 */
final class MethodRunnable implements Runnable {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final ConsumerImpl consumer;
    private final UUID methodUuid;
    private final ConsumerConfig consumerConfig;
    private final List<MethodReceivingCallback> methodEventListeners;
    private final ConsumerHandlers consumerHandlers;
    private final String methodKey;

    MethodRunnable(ConsumerImpl consumer, ConsumerConfig consumerConfig, List<MethodReceivingCallback> methodEventListeners, ConsumerHandlers consumerHandlers, UUID methodUuid) {
        this.consumer = consumer;
        this.consumerConfig = consumerConfig;
        this.methodEventListeners = methodEventListeners;
        this.consumerHandlers = consumerHandlers;
        this.methodUuid = methodUuid;
        this.methodKey = consumer.getKeyNaming().methodDetailsKey(methodUuid);
    }

    @Override
    public void run() {
        try (final Jedis store = consumerConfig.getPool().getResource()) {

            if (!steal(store)) {
                LOGGER.debug("Could not steal {}", methodUuid);
                // some other instance has already took this item, we do nothing
                return;
            }

            MethodRequestFrame mrf = null;

            // itt egy masik try-catch van, mert csak akkor irhatom vissza, hogy nem sikerult, ha mar egyem az ownership.
            try {
                final Optional<MethodRequestFrame> frameOp = DataHandling.readMethodFrame(store, consumer.getKeyNaming(), consumerHandlers.getMethodDescriptorResolver(), methodUuid);

                if (frameOp.isPresent()) {
                    mrf = frameOp.get();
                    process(store, mrf);
                } else {
                    LOGGER.debug("Could not read from method {}", methodKey);
                    // lejart a metodus?
                }
            } catch (Throwable t) {
                LOGGER.error("Exception happened when sending method");
                debugMethodFrame(LOGGER, mrf);
                LOGGER.error("Method exception: ", t);
                writeMethodFailure(store, t);

                if (mrf != null) {
                    callFailureListeners(mrf, t);
                }
            } finally {
                respond(store);
            }
        }
    }

    private void process(Jedis store, MethodRequestFrame mrf) throws SerializationException {

        final MethodDescriptor methodMarker = mrf.getMethodMarker();

        store.zrem(consumer.getKeyNaming().unprocessedEventsByGroupKey(methodMarker.getMethodGroupName()), mrf.getIdentifier().toString());
        // esemenykezelok futtatasa
        methodEventListeners.forEach(x -> {
            try {
                x.onRequestReceived(mrf);
            } catch (Throwable t) {
                LOGGER.error("Error when running method sending event listener {} for method {}", x, methodUuid);
            }
        });
        LOGGER.debug("Calling method processor for {}/{}", methodMarker.getMethodGroupName(), methodMarker.getMethodName());

        //noinspection unchecked
        final Object response = consumer.getMethodProcessorAdapter().call(methodMarker, mrf.getMethodData(), mrf.getMetaData());
        LOGGER.debug("Called method processor for {}/{}", methodMarker.getMethodGroupName(), methodMarker.getMethodName());

        // TODO: use multi/exec here to writre statuses and stuff.
        store.hset(methodKey, DataHandling.MethodResponseKeys.RESPONSE.name(), SerializeHelper.serializeObject(response));
        store.hset(methodKey, DataHandling.MethodResponseKeys.STATUS.name(), DataHandling.Statuses.PROCESSED.name());

        methodEventListeners.forEach(x -> {
            try {
                x.onRequestProcessedSuccessfully(mrf, response);
            } catch (Throwable t) {
                debugMethodFrame(LOGGER, mrf);
                LOGGER.error("Error when running method sending event listener.", t);
            }
        });

        LOGGER.debug("Written response to method {}/{} to {}",
                methodMarker.getMethodGroupName(), methodMarker.getMethodName(), methodKey);
    }

    /**
     * On case of callFailureListeners we run callbacks and write callFailureListeners code.
     */
    private void callFailureListeners(MethodRequestFrame mrf, Throwable t) {
        // esemenykezelok futtatasa
        methodEventListeners.forEach(x -> {
            try {
                x.onRequestProcessingFailure(mrf, t);
            } catch (Throwable e) {
                LOGGER.error("Error when running method sending event listener for {} on {}", e);
            }
        });
    }

    /**
     * Sets timeout on method key and notifies reqester module.
     */
    private void respond(Jedis store) {
        if (!store.getClient().isInMulti()) {

            final String methodKey = consumer.getKeyNaming().methodDetailsKey(methodUuid);

            // hogy nehogy lejarjon mire megjon a valasz!
            store.expire(methodKey, 15);

            String responseNotificationChannel = consumer.getKeyNaming().getMessageResponseNotificationChannel(methodUuid);
            LOGGER.debug("Notifying response on {} with {}", responseNotificationChannel, methodUuid.toString());

            store.publish(responseNotificationChannel, methodUuid.toString());

        } else {
            // this is an illegal state. we should not send a notification because it makes no sense.
            LOGGER.error("A Jedis Multi has been interrupted!");
        }
    }

    /**
     * Tries to mark method as being processed by current instance. Returns true iff succeeded.
     */
    private boolean steal(Jedis store) {
        final String methodKey = consumer.getKeyNaming().methodDetailsKey(methodUuid);
        LOGGER.debug("Trying to steal from {}", methodUuid);
        return 0 != store.hsetnx(methodKey, "owner", consumer.getConsumerIdentity().getIdentifier());
    }

    private void writeMethodFailure(Jedis store, Throwable t) {
        LOGGER.error("Hiba a metodus feldolgozasa kozben!", t);

        final Transaction tx = store.multi();
        tx.hset(methodKey, DataHandling.MethodResponseKeys.STATUS.name(), "ERROR");
        tx.hset(methodKey, DataHandling.MethodResponseKeys.EXCEPTION_CLASS.name(), t.getClass().getName());
        tx.hset(methodKey, DataHandling.MethodResponseKeys.EXCEPTION_MESSAGE.name(), t.getMessage());
        tx.exec();
    }
}
