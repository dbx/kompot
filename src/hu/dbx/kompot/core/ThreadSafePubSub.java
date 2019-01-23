package hu.dbx.kompot.core;

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
            }

            subscribed.add(channel);

            listener.onSubscribe(channel, subscribedChannels);
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            if (subscribedChannels == 0) {
                stopLatch.countDown();
            }

            subscribed.remove(channel);

            listener.onUnsubscribe(channel, subscribedChannels);
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
    public synchronized void startWithChannels(String... channels) throws InterruptedException {

        subscribed.addAll(Arrays.asList(channels));

        daemonThread.start();

        startLatch.await();
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

    public interface Listener {
        void onMessage(String channel, String message);

        void onSubscribe(String channel, int subscribedChannels);

        void onUnsubscribe(String channel, int subscribedChannels);
    }
}


