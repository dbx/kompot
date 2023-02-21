package hu.dbx.kompot.ng;

import hu.dbx.kompot.TestRedis;
import org.junit.ClassRule;

public abstract class AbstractRedisTest {
    @ClassRule public static TestRedis redis = TestRedis.build();
}
