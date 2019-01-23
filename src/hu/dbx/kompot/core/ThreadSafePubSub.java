package hu.dbx.kompot.core;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

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

    private final Set<String> onceSubscribed = new ConcurrentSkipListSet<>();

    // do not user ConcurrentHashMap here because keys will not be visible on both threads immediately
    private final Map<String, CountDownLatch> channelToLatch = Collections.synchronizedMap(new HashMap<>());

    public ThreadSafePubSub(JedisPool pool, Listener listener) {
        this.jedisPool = pool;
        this.listener = listener;
    }

    private final JedisPubSub pubSub = new JedisPubSub() {

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            if (channelToLatch.containsKey(channel)) {
                channelToLatch.get(channel).countDown();
            }

            subscribed.add(channel);

            listener.onSubscribe(channel, subscribedChannels);
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            if (channelToLatch.containsKey(channel)) {
                channelToLatch.get(channel).countDown();
            }

            subscribed.remove(channel);

            listener.onUnsubscribe(channel, subscribedChannels);
        }

        @Override
        public void onMessage(String channel, String message) {
            if (poison.equals(channel)) {
                pubSub.unsubscribe();
                for (CountDownLatch latch : channelToLatch.values()) {
                    latch.countDown();
                }
            } else {

                if (onceSubscribed.contains(channel)) {
                    onceSubscribed.remove(channel);
                    pubSub.unsubscribe(channel);
                }

                listener.onMessage(channel, message);
            }
        }
    };

    /**
     * Starts this component by subscribint to the given set of channels.
     */
    public synchronized void startWithChannels(String... channels) throws InterruptedException {
        for (String c : channels) {
            channelToLatch.put(c, new CountDownLatch(1));
        }

        daemonThread.start();

        for (String channel : channels) {
            channelToLatch.get(channel).await();
            channelToLatch.remove(channel);
        }
    }

    @Override
    public void run() {
        try (Jedis resource = jedisPool.getResource()) {
            final Set<String> chs = new HashSet<>(channelToLatch.keySet());
            chs.add(poison);

            resource.subscribe(pubSub, chs.toArray(new String[]{}));

            // itt megall a rendszer es blokkol!!!
        }
    }

    /**
     * Addig blokkol amig le nem all a rendszer.
     */
    public synchronized void unsubscrubeAllAndStop() throws InterruptedException {

        channelToLatch.put(poison, new CountDownLatch(1));

        for (String channel : new HashSet<>(subscribed)) {
            channelToLatch.put(channel, new CountDownLatch(1));
        }

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(poison, "now");
        }

        for (Map.Entry<String, CountDownLatch> entry : channelToLatch.entrySet()) {
            entry.getValue().await();
        }
    }

    public interface Listener {
        void onMessage(String channel, String message);

        void onSubscribe(String channel, int subscribedChannels);

        void onUnsubscribe(String channel, int subscribedChannels);
    }
}


