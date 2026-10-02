package ru.refiq.config;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class InfrastructureReadiness {

    private final AtomicBoolean ready = new AtomicBoolean(false);

    public void markReady() {
        ready.set(true);
    }

    public boolean isReady() {
        return ready.get();
    }
}
