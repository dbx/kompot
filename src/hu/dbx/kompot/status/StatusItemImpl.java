package hu.dbx.kompot.status;

public final class StatusItemImpl implements StatusReport.StatusItem {

    private final String name;
    private final String description;
    private final String statusMessage;
    private final String errorMessage;

    public StatusItemImpl(String name, String description, String statusMessage, String errorMessage) {
        this.name = name;
        this.description = description;
        this.statusMessage = statusMessage;
        this.errorMessage = errorMessage;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getStatusMessage() {
        return statusMessage;
    }

    @Override
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public boolean isOk() {
        return errorMessage == null;
    }
}
