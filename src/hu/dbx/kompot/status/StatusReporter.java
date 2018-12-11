package hu.dbx.kompot.status;

import java.util.concurrent.Callable;

public class StatusReporter {

    private final String name;
    private final String description;
    private final Callable<StatusResult> endpoint;

    public StatusReporter(String name, String description, Callable<StatusResult> endpoint) {
        this.name = name;
        this.description = description;
        this.endpoint = endpoint;
    }

    /**
     * Short name of the status report.
     */
    public String getName() {
        return name;
    }

    /**
     * Short description of the status report.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Function to get the actual status
     */
    public Callable<StatusResult> getEndpoint() {
        return endpoint;
    }



    public static final class StatusResult {

        private final String errorMessage;

        private StatusResult(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public static StatusResult resultOk() {
            return new StatusResult(null);
        }

        public static StatusResult resultError(String errorMsg) {
            return new StatusResult(errorMsg);
        }

        /**
         * Error description used if status is not ok
         */
        public String getErrorMessage() {
            return errorMessage;
        }

        /**
         * Is the subsystem status acceptable?
         */
        public boolean isOk() {
            return getErrorMessage() == null;
        }
    }
}


