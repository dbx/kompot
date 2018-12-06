package hu.dbx.kompot;

import hu.dbx.kompot.impl.LoggerUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Redis kapcsolatot ad az egysegtesztek futtatasahoz.
 */
public class TestRedis extends ExternalResource {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final URI uri;
    private JedisPool pool;

    private static final String ENV_KEY = "KOMPOT_REDIS_URI";
    private static final String DEFAULT_REDIS_URI = "redis://localhost:6379/13";

    public static TestRedis build() {
        try {
            return new TestRedis();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public TestRedis() throws URISyntaxException {
        if (System.getenv().containsKey(ENV_KEY)) {
            uri = new URI(System.getenv(ENV_KEY));
        } else {
            uri = new URI(DEFAULT_REDIS_URI);
        }
    }

    @Override
    public void before() {
        pool = new JedisPool(uri);
        try (Jedis jedis = pool.getResource()) {
            LOGGER.info(jedis.info());
            jedis.flushDB();
        }
    }

    @Override
    public void after() {
        pool.close();
    }

    public JedisPool getJedisPool() {
        return pool;
    }

    public URI getConnectionURI() {
        return uri;
    }
}
