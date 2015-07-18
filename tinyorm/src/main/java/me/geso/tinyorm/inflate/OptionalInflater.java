package me.geso.tinyorm.inflate;

import me.geso.tinyorm.trigger.Inflater;

import java.util.Optional;

public class OptionalInflater implements Inflater {
    @Override
    public Object inflate(final Object value) {
        return Optional.ofNullable(value);
    }
}
