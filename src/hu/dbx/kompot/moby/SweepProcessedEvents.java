package hu.dbx.kompot.moby;

import hu.dbx.kompot.consumer.async.EventDescriptor;

import java.util.Map;

/**
 * Ha egy esemeny teljesen fel van dolgozva, beirja egy adatbazisba a vegen.
 */
public final class SweepProcessedEvents {

    // mapet csinal amit be lehet szurni az adatbazisba
    protected <TReq> Map<String, Object> toMap(EventDescriptor<TReq> marker, TReq request) {
        return null;
    }

}
