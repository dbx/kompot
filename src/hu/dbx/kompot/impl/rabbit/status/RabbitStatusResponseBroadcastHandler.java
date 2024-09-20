package hu.dbx.kompot.impl.rabbit.status;

import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.status.StatusReport;

public class RabbitStatusResponseBroadcastHandler implements SelfDescribingBroadcastProcessor<StatusReport> {

    public static final BroadcastDescriptor<StatusReport> DESCRIPTOR = BroadcastDescriptor.of("KMPT_SAY_HELLO_RESPONSE", StatusReport.class);

    private final RabbitStatusReportingAction statusReportingAction;

    public RabbitStatusResponseBroadcastHandler(RabbitStatusReportingAction rabbitStatusReportingAction) {
        this.statusReportingAction = rabbitStatusReportingAction;
    }

    @Override
    public BroadcastDescriptor<StatusReport> getBroadcastDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public void handle(StatusReport statusReport) {
        statusReportingAction.addStatusReport(statusReport);
    }

}
