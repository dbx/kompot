package hu.dbx.kompot.core;

import hu.dbx.kompot.status.StatusReport;
import hu.dbx.kompot.status.StatusReporter;

import java.util.List;

public interface StatusReportingAction {

    void registerStatusReporter(StatusReporter reporter);

    List<StatusReport> findGlobalStatuses();

}
