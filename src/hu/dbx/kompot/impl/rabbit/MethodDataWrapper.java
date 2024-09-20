package hu.dbx.kompot.impl.rabbit;

import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.moby.MetaDataHolder;

public class MethodDataWrapper extends DataWrapper {

    private DataHandling.Statuses status;
    private String exceptionClass;
    private String exceptionMessage;

    public MethodDataWrapper() {
    }

    public MethodDataWrapper(byte[] data, MetaDataHolder metaDataHolder) {
        super(data, metaDataHolder);
    }

    public DataHandling.Statuses getStatus() {
        return status;
    }

    public void setStatus(DataHandling.Statuses status) {
        this.status = status;
    }

    public String getExceptionClass() {
        return exceptionClass;
    }

    public void setExceptionClass(String exceptionClass) {
        this.exceptionClass = exceptionClass;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }
}
