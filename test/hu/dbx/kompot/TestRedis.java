package hu.dbx.kompot;

import hu.dbx.kompot.impl.LoggerUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Redis kapcsolatot ad az egysegtesztek futtatasahoz.
 */
public class TestRedis extends ExternalResource {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final URI uri;
    private JedisPool pool;

    public static TestRedis build() {
        try {
            return new TestRedis();
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public TestRedis() throws IOException, URISyntaxException {
//        redisServer = new RedisServer(6379);
        uri = new URI("redis://localhost:" + 6379 + "/13");
    }

    @Override
    public void before() {
//        redisServer.start();
        pool = new JedisPool(uri);
        try (Jedis jedis = pool.getResource()) {
            LOGGER.info(jedis.info());
            jedis.flushDB();
        }
    }

    @Override
    public void after() {
//        redisServer.stop();
    }

    public JedisPool getJedisPool() {
        return pool;
    }

    public URI getConnectionURI() {
        return uri;
    }
}
