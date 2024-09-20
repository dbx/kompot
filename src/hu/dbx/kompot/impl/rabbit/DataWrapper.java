package hu.dbx.kompot.impl.rabbit;

import hu.dbx.kompot.moby.MetaDataHolder;

public class DataWrapper {

    private byte[] data;
    private MetaDataHolder metaDataHolder;

    public DataWrapper() {
    }

    public DataWrapper(final byte[] data, final MetaDataHolder metaDataHolder) {
        this.data = data;
        this.metaDataHolder = metaDataHolder;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public MetaDataHolder getMetaDataHolder() {
        if (metaDataHolder == null) {
            metaDataHolder = new MetaDataHolder();
        }
        return metaDataHolder;
    }

    public void setMetaDataHolder(MetaDataHolder metaDataHolder) {
        this.metaDataHolder = metaDataHolder;
    }

}
