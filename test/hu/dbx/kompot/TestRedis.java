package hu.dbx.kompot;

import hu.dbx.kompot.impl.LoggerUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Redis kapcsolatot ad az egysegtesztek futtatasahoz.
 */
public class TestRedis extends ExternalResource {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final GenericContainer redis;

    private final URI uri;
    private JedisPool pool;

    private static final String ENV_KEY = "KOMPOT_REDIS_URI";
    public static TestRedis build() {
        return new TestRedis();
    }

    private TestRedis() {
        try {
            if (System.getenv().containsKey(ENV_KEY)) {
                redis = null;
                uri = new URI(System.getenv(ENV_KEY));
            } else {
                LOGGER.debug("Initializing redis docker container");
                redis = new GenericContainer(DockerImageName.parse("redis:5.0.3-alpine"))
                        .withExposedPorts(6379);
                redis.start();
                uri = new URI("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379) + "/13");
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void before() {
        pool = new JedisPool(uri);
        try (Jedis jedis = pool.getResource()) {
            LOGGER.trace(jedis.info());
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
