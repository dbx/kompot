package hu.dbx.kompot.impl.jedis;

import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.Listener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread safe facade for JedisPubSub
 */
public final class ThreadSafePubSub implements Runnable {

    private final JedisPool jedisPool;
    private final Listener listener;

    private final Set<String> subscribed = Collections.synchronizedSet(new HashSet<>());

    private final Thread daemonThread = new Thread(this);

    // receiving an event on this channel stops the component
    private final String poison = UUID.randomUUID().toString();

    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    private final AtomicInteger expectedChannels = new AtomicInteger(-1);

    public ThreadSafePubSub(JedisPool pool, Listener listener) {
        this.jedisPool = pool;
        this.listener = listener;
    }


    private final JedisPubSub pubSub = new JedisPubSub() {

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            if (subscribedChannels == expectedChannels.get()) {
                startLatch.countDown();
                listener.afterStarted();
            }

            subscribed.add(channel);
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            if (subscribedChannels == 0) {
                stopLatch.countDown();
                listener.afterStopped();
            }

            subscribed.remove(channel);
        }

        @Override
        public void onMessage(String channel, String message) {
            if (poison.equals(channel)) {
                pubSub.unsubscribe();
            } else {
                listener.onMessage(channel, message);
            }
        }
    };

    /**
     * Starts this component by subscribint to the given set of channels.
     */
    public synchronized void startWithChannels(ConsumerIdentity consumerIdentity, Set<String> supportedBroadcastCodes) throws InterruptedException {

        subscribed.addAll(Arrays.asList(getPubSubChannels(consumerIdentity, supportedBroadcastCodes)));

        daemonThread.start();

        startLatch.await();
    }

    /**
     * Osszeszedi az osszes figyelt csatornat.
     */
    private String[] getPubSubChannels(ConsumerIdentity consumerIdentity, Set<String> supportedBroadcastCodes) {
        List<String> channels = new LinkedList<>();

        // nekem cimzett esemenyek
        channels.add("e:" + consumerIdentity.getEventGroup());

        // nekem cimzett metodusok
        channels.add("m:" + consumerIdentity.getMessageGroup());

        // tamogatott broadcast uzenet tipusok
        supportedBroadcastCodes.forEach(broadcastCode -> channels.add("b:" + broadcastCode));

        // szemelyesen nekem cimzett visszajelzesek
        channels.add("id:" + consumerIdentity.getIdentifier());

        return channels.toArray(new String[]{});
    }

    @Override
    public void run() {
        try (Jedis resource = jedisPool.getResource()) {
            subscribed.add(poison);
            expectedChannels.set(subscribed.size());

            resource.subscribe(pubSub, subscribed.toArray(new String[]{}));
            // the worker thread blocks here.
            // maybe add logging and error handling for network errors.
        }
    }

    /**
     * Addig blokkol amig le nem all a rendszer.
     */
    public synchronized void unsubscrubeAllAndStop() throws InterruptedException {

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(poison, "now");
        }

        stopLatch.await();
    }

}
