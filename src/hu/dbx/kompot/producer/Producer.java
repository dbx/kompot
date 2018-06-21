package hu.dbx.kompot.producer;

import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;

import java.util.concurrent.CompletableFuture;

/**
 * A kliens az esemenyek/uzenetek forrasa a rendszerben.
 */
public interface Producer {

    /**
     * Sends an asynchronous event.
     * Events are sent to EVERY event groups for this kind of event.
     *
     * @param entityCreated a kikuldendo esemeny leiroja
     * @param request       a kikuldendo esemeny adattartama
     * @param <TReq>        az esemeny adattartam tipusa
     * @throws NullPointerException   when any argument is null
     * @throws SerializationException when payload can not be serialized for some reason
     */
    <TReq> void sendEvent(EventDescriptor<TReq> entityCreated, TReq request) throws SerializationException;


    /**
     * @return service to get event group names
     */
    EventGroupProvider getEventGroupProvider();


    /**
     * Sends an asynchronous message.
     * Messages are sent to ONE message group of this kind of message.
     *
     * @param marker     a kuldendo metodus leiroja
     * @param methodData a kuldendo metodus parameter objektuma
     * @param <TReq>     a parameter tipusa
     * @param <TRes>     a valasz tipusa
     * @param timeoutMs a hívás timeoutja ms-ben
     * @return a future that completes when method response has arrived.
     * @throws SerializationException   when could not serialize method data
     * @throws IllegalArgumentException when any argument is null
     */
    <TReq, TRes> CompletableFuture<TRes> sendMessage(MethodDescriptor<TReq, TRes> marker, TReq methodData, long timeoutMs) throws SerializationException;


    /**
     * Kikuld egy broadcast uzenetet annak aki figyel
     *
     * @throws SerializationException ha nem tudtuk az adatot szerializalni
     */
    <TReq> void broadcast(BroadcastDescriptor<TReq> descriptor, TReq broadcastData) throws SerializationException;


    ProducerIdentity getProducerIdentity();
}
