package hu.dbx.kompot.consumer.sync.handler;

import hu.dbx.kompot.consumer.sync.MethodDescriptor;

import java.util.function.Function;

public interface SelfDescribingMethodProcessor<TReq, TRes> {

    MethodDescriptor<TReq, TRes> getMethodMarker();

    TRes handle(TReq request);

    static <TReq, TRes> SelfDescribingMethodProcessor<TReq, TRes> of(MethodDescriptor<TReq, TRes> m, Function<TReq, TRes> map) {
        return new SelfDescribingMethodProcessor<TReq, TRes>() {
            @Override
            public MethodDescriptor<TReq, TRes> getMethodMarker() {
                return m;
            }

            @Override
            public TRes handle(TReq request) {
                return map.apply(request);
            }
        };
    }
}
