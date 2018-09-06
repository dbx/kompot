package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodReceivingCallback;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

final class MethodRunnable implements Runnable {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final ConsumerImpl consumer;
    private final UUID methodUuid;
    private final ConsumerConfig consumerConfig;
    private final List<MethodReceivingCallback> methodEventListeners;
    private final ConsumerHandlers consumerHandlers;

    MethodRunnable(ConsumerImpl consumer, ConsumerConfig consumerConfig, List<MethodReceivingCallback> methodEventListeners, ConsumerHandlers consumerHandlers, UUID methodUuid) {
        this.consumer = consumer;
        this.consumerConfig = consumerConfig;
        this.methodEventListeners = methodEventListeners;
        this.consumerHandlers = consumerHandlers;
        this.methodUuid = methodUuid;
    }

    @Override
    public void run() {
        try (final Jedis store = consumerConfig.getPool().getResource()) {
            // try to assign this method to self

            // a metodus cucca ide megy.
            final String methodKey = consumer.getKeyNaming().methodDetailsKey(methodUuid);

            // ebben fogom tarolni a metodus hivas reszleteit.
            MethodRequestFrame frame = null;

            LOGGER.debug("Trying to steal from {}", methodKey);
            Long result = store.hsetnx(methodKey, "owner", consumer.getConsumerIdentity().getIdentifier());

            if (result == 0) {
                LOGGER.debug("Could not steal {}", methodKey);
                // some other instance has already took this item, we do nothing
                return;
            }

            // itt egy masik try-catch van, mert csak akkor irhatom vissza, hogy nem sikerult, ha mar egyem az ownership.
            try {
                final Optional<MethodRequestFrame> frameOp = DataHandling.readMethodFrame(store, consumer.getKeyNaming(), consumerHandlers.getMethodDescriptorResolver(), methodUuid);

                if (!frameOp.isPresent()) {
                    LOGGER.debug("Could not read from method {}", methodKey);
                    // lejart a metodus?
                    return;
                }

                frame = frameOp.get();
                final MethodRequestFrame mrf = frame;
                final MethodDescriptor methodMarker = frame.getMethodMarker();

                store.zrem(consumer.getKeyNaming().unprocessedEventsByGroupKey(methodMarker.getMethodGroupName()), frame.getIdentifier().toString());
                // esemenykezelok futtatasa
                methodEventListeners.forEach(x -> {
                    try {
                        x.onRequestReceived(mrf);
                    } catch (Throwable t) {
                        LOGGER.error("Error when running method sending event listener {} for method {}", x, methodUuid);
                    }
                });
                LOGGER.debug("Calling method processor for {}/{}", methodMarker.getMethodGroupName(), methodMarker.getMethodName());
                // siker eseten visszairjuk a sikeres vackot

                // TODO: irjuk be a folyamatban levo esemenyes soraba!
                //noinspection unchecked
                final Object response = consumer.getMethodProcessorAdapter().call(methodMarker, frame.getMethodData(), frame.getMetaData());
                LOGGER.debug("Called method processor for {}/{}", methodMarker.getMethodGroupName(), methodMarker.getMethodName());

                // TODO: use multi/exec here to writre statuses and stuff.
                store.hset(methodKey, DataHandling.MethodResponseKeys.RESPONSE.name(), SerializeHelper.serializeObject(response));
                store.hset(methodKey, DataHandling.MethodResponseKeys.STATUS.name(), DataHandling.Statuses.PROCESSED.name());

                // esemenykezelok futtatasa
                methodEventListeners.forEach(x -> {
                    try {
                        x.onRequestProcessedSuccessfully(mrf, response);
                    } catch (Throwable t) {
                        LOGGER.error("Error when running method sending event listener.", t);
                    }
                });

                LOGGER.debug("Written response to method {}/{} to {}",
                        methodMarker.getMethodGroupName(), methodMarker.getMethodName(), methodKey);
            } catch (Throwable t) {
                LOGGER.error("Exception happened when sending method " + methodUuid, t);
                writeMethodFailure(store, methodKey, t);

                if (frame != null) {
                    final MethodRequestFrame mrf = frame;
                    // esemenykezelok futtatasa
                    methodEventListeners.forEach(x -> {
                        try {
                            x.onRequestProcessingFailure(mrf, t);
                        } catch (Throwable e) {
                            LOGGER.error("Error when running method sending event listener for {} on {}", e);
                        }
                    });
                }
            } finally {
                // hogy nehogy lejarjon mire megjon a valasz!
                store.expire(methodKey, 15);

                String responseNotificationChannel = consumer.getKeyNaming().getMessageResponseNotificationChannel(methodUuid);
                LOGGER.debug("Notifying response on {} with {}", responseNotificationChannel, methodUuid.toString());

                store.publish(responseNotificationChannel, methodUuid.toString());
            }
        }
    }

    private void writeMethodFailure(Jedis store, String methodKey, Throwable t) {
        LOGGER.error("Hiba a metodus feldolgozasa kozben!", t);

        final Transaction tx = store.multi();
        tx.hset(methodKey, DataHandling.MethodResponseKeys.STATUS.name(), "ERROR");
        tx.hset(methodKey, DataHandling.MethodResponseKeys.EXCEPTION_CLASS.name(), t.getClass().getName());
        tx.hset(methodKey, DataHandling.MethodResponseKeys.EXCEPTION_MESSAGE.name(), t.getMessage());
        tx.exec();
    }
}
