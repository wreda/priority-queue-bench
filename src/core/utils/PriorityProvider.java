package core.utils;

public interface PriorityProvider {
        PriorityTuple getPriority();
        int getBatchSize();
}