package hu.dbx.kompot.status;

import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import org.slf4j.Logger;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Every module is subscribed to this broadcast by default.
 * <p>
 * When received the module writes its status to the response key found in payload.
 */
public abstract class StatusRequestBroadcastHandler implements SelfDescribingBroadcastProcessor<Map> {

    protected static final Logger LOGGER = LoggerUtils.getLogger();

    // @SuppressWarnings("unchecked")
    public static final BroadcastDescriptor<Map> DESCRIPTOR = BroadcastDescriptor.of("KMPT_SAY_HELLO", Map.class);

    protected final Supplier<StatusReport> statusReportFactory;
    protected final ConsumerConfig config;

    public StatusRequestBroadcastHandler(Supplier<StatusReport> statusReportFactory, ConsumerConfig config) {
        this.statusReportFactory = statusReportFactory;
        this.config = config;
    }

    @Override
    public BroadcastDescriptor<Map> getBroadcastDescriptor() {
        return DESCRIPTOR;
    }

    public abstract void handle(Map request);

}
