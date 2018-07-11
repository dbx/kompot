package hu.dbx.kompot.consumer.sync.handler;

import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.moby.MetaDataHolder;

import java.util.function.BiFunction;
import java.util.function.Function;

public interface SelfDescribingMethodProcessor<TReq, TRes> {

    MethodDescriptor<TReq, TRes> getMethodMarker();

    TRes handle(TReq request, MetaDataHolder metaDataHolder);

    static <TReq, TRes> SelfDescribingMethodProcessor<TReq, TRes> of(MethodDescriptor<TReq, TRes> m, BiFunction<TReq, MetaDataHolder, TRes> map) {
        return new SelfDescribingMethodProcessor<TReq, TRes>() {
            @Override
            public MethodDescriptor<TReq, TRes> getMethodMarker() {
                return m;
            }

            @Override
            public TRes handle(TReq request, MetaDataHolder metaDataHolder) {
                return map.apply(request, metaDataHolder);
            }
        };
    }

    static <TReq, TRes> SelfDescribingMethodProcessor<TReq, TRes> of(MethodDescriptor<TReq, TRes> m, Function<TReq, TRes> map) {
        return of(m, (req, meta) -> map.apply(req));
    }
}
