package hu.dbx.kompot.consumer.sync.handler;

import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptorResolver;
import hu.dbx.kompot.moby.MetaDataHolder;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public final class DefaultMethodProcessorAdapter implements MethodDescriptorResolver {

    private Set<SelfDescribingMethodProcessor> processors = new HashSet<>();

    public <TReq, TRes> TRes call(MethodDescriptor<TReq, TRes> marker, TReq request, MetaDataHolder meta) {
        final Optional<SelfDescribingMethodProcessor> p = processors.stream().filter(x -> x.getMethodMarker().equals(marker)).findAny();
        if (!p.isPresent())
            throw new IllegalArgumentException("Can not find processor for method!");

        //noinspection unchecked
        return (TRes) p.get().handle(request, meta);
    }

    public void register(SelfDescribingMethodProcessor processor) {
        processors.add(processor);
    }

    @Override
    public Optional<MethodDescriptor> resolveMarker(String methodName) {
        return processors.stream()
                .map((Function<SelfDescribingMethodProcessor, MethodDescriptor>) SelfDescribingMethodProcessor::getMethodMarker)
                .filter(p -> p.getMethodName().equalsIgnoreCase(methodName))
                .findAny();
    }
}
