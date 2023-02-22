package hu.dbx.kompot;

import hu.dbx.kompot.impl.LoggerUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Redis kapcsolatot ad az egysegtesztek futtatasahoz.
 */
public class TestRedis extends ExternalResource {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final GenericContainer<?> redis;

    private final URI uri;
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
                redis = new GenericContainer<>(DockerImageName.parse("redis:5.0.3-alpine"))
                        .withExposedPorts(6379)
                        .waitingFor(Wait.forListeningPort())
                        .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("REDIS"));
                redis.start();
                uri = new URI("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379) + "/13");
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void before() {
        //flush the db
        try (Jedis jedis = new Jedis(uri)) {
            LOGGER.trace(jedis.info());
            jedis.flushDB();
        }
    }

    public URI getConnectionURI() {
        return uri;
    }
}
