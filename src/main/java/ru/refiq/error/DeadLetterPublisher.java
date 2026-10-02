package ru.refiq.error;

public interface DeadLetterPublisher {

    void publish(DeadLetter letter);
}
