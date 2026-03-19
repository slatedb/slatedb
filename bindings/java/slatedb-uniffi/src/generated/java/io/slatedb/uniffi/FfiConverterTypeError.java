package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeError implements FfiConverterRustBuffer<Error> {
    INSTANCE;

    @Override
    public Error read(ByteBuffer buf) {

        return switch(buf.getInt()) {
            case 1 -> new Error.Transaction(
                FfiConverterString.INSTANCE.read(buf)
                );
            case 2 -> new Error.Closed(
                FfiConverterTypeCloseReason.INSTANCE.read(buf),
                FfiConverterString.INSTANCE.read(buf)
                );
            case 3 -> new Error.Unavailable(
                FfiConverterString.INSTANCE.read(buf)
                );
            case 4 -> new Error.Invalid(
                FfiConverterString.INSTANCE.read(buf)
                );
            case 5 -> new Error.Data(
                FfiConverterString.INSTANCE.read(buf)
                );
            case 6 -> new Error.Internal(
                FfiConverterString.INSTANCE.read(buf)
                );
            default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
        };
    }

    @Override
    public long allocationSize(Error value) {
        return switch(value) {
            case Error.Transaction x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case Error.Closed x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterTypeCloseReason.INSTANCE.allocationSize(x.reason)
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case Error.Unavailable x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case Error.Invalid x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case Error.Data x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case Error.Internal x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
        };
    }

    @Override
    public void write(Error value, ByteBuffer buf) {
        switch(value) {
            case Error.Transaction x -> {
                buf.putInt(1);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case Error.Closed x -> {
                buf.putInt(2);
                FfiConverterTypeCloseReason.INSTANCE.write(x.reason, buf);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case Error.Unavailable x -> {
                buf.putInt(3);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case Error.Invalid x -> {
                buf.putInt(4);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case Error.Data x -> {
                buf.putInt(5);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case Error.Internal x -> {
                buf.putInt(6);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
        };
    }
}


