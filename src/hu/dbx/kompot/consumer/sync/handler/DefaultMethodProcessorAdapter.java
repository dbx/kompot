package hu.dbx.kompot.consumer.sync.handler;

import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptorResolver;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public final class DefaultMethodProcessorAdapter implements MethodDescriptorResolver {

    private Set<SelfDescribingMethodProcessor> processors = new HashSet<>();

    @SuppressWarnings({"unchecked", "unused"})
    public <TReq, TRes> TRes call(MethodDescriptor<TReq, TRes> marker, TReq request) {
        final Optional<SelfDescribingMethodProcessor> p = processors.stream().filter(x -> x.getMethodMarker().equals(marker)).findAny();
        if (!p.isPresent())
            throw new IllegalArgumentException("Can not find processor for method!");

        return (TRes) p.get().handle(request);
    }

    public void register(SelfDescribingMethodProcessor processor) {
        processors.add(processor);
    }

    @Override
    public Optional<MethodDescriptor> resolveMarker(String methodName) {
        return processors.stream()
                .map(p -> p.getMethodMarker())
                .filter(p -> p.getMethodName().equalsIgnoreCase(methodName))
                .findAny();
    }
}
