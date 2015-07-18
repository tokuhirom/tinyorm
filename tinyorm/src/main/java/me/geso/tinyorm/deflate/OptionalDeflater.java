package me.geso.tinyorm.deflate;

import me.geso.tinyorm.trigger.Deflater;

import java.util.Optional;

public class OptionalDeflater implements Deflater {
    @Override
    @SuppressWarnings("unchecked")
    public Object deflate(final Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Optional) {
            return ((Optional) value).orElse(null);
        } else {
            throw new IllegalArgumentException("OptionalDeflater supports only Optional");
        }
    }
}
