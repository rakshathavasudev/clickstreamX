package com.adplatform.datagen;

import com.adplatform.clickstream.proto.ClickEvent;
import com.adplatform.clickstream.proto.ClickStreamRequest;
import com.adplatform.clickstream.proto.Context;
import com.adplatform.clickstream.proto.DataGeneratorServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@GrpcService
public class ClickStreamService extends DataGeneratorServiceGrpc.DataGeneratorServiceImplBase {

    private final Random random = new Random();

    @Override
    public void streamClicks(
            ClickStreamRequest request,
            StreamObserver<ClickEvent> responseObserver) {

        int rate = Math.max(1, request.getEventsPerSecond());
        long intervalMillis = 1000L / rate;

        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();

        executor.scheduleAtFixedRate(() -> {
            try {
                ClickEvent event = generateEvent();
                responseObserver.onNext(event);
            } catch (Exception e) {
                responseObserver.onError(e);
                executor.shutdown();
            }
        }, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private ClickEvent generateEvent() {
        boolean clicked = random.nextDouble() < 0.08; // ~8% CTR

        return ClickEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setUserId("user-" + random.nextInt(100_000))
                .setAdId("ad-" + random.nextInt(5_000))
                .setEventTimestamp(Instant.now().toEpochMilli())
                .setClicked(clicked)
                .setBidPrice(0.5 + random.nextDouble() * 2.0)
                .setContext(generateContext())
                .build();
    }

    private Context generateContext() {
        return Context.newBuilder()
                .setDeviceType(randomFrom("mobile", "desktop", "tablet"))
                .setOs(randomFrom("android", "ios", "windows", "macos"))
                .setCountry(randomFrom("US", "IN", "UK", "DE"))
                .setRegion(randomFrom("CA", "NY", "TX", "WA"))
                .build();
    }

    private String randomFrom(String... values) {
        return values[random.nextInt(values.length)];
    }
}
