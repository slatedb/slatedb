package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeSlatedbError implements FfiConverterRustBuffer<SlatedbException> {
    INSTANCE;

    @Override
    public SlatedbException read(ByteBuffer buf) {

        return switch(buf.getInt()) {
            case 1 -> new SlatedbException.Transaction(
                FfiConverterString.INSTANCE.read(buf)
                );
            case 2 -> new SlatedbException.Closed(
                FfiConverterTypeCloseReason.INSTANCE.read(buf),
                FfiConverterString.INSTANCE.read(buf)
                );
            case 3 -> new SlatedbException.Unavailable(
                FfiConverterString.INSTANCE.read(buf)
                );
            case 4 -> new SlatedbException.Invalid(
                FfiConverterString.INSTANCE.read(buf)
                );
            case 5 -> new SlatedbException.Data(
                FfiConverterString.INSTANCE.read(buf)
                );
            case 6 -> new SlatedbException.Internal(
                FfiConverterString.INSTANCE.read(buf)
                );
            default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
        };
    }

    @Override
    public long allocationSize(SlatedbException value) {
        return switch(value) {
            case SlatedbException.Transaction x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case SlatedbException.Closed x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterTypeCloseReason.INSTANCE.allocationSize(x.reason)
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case SlatedbException.Unavailable x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case SlatedbException.Invalid x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case SlatedbException.Data x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            case SlatedbException.Internal x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
        };
    }

    @Override
    public void write(SlatedbException value, ByteBuffer buf) {
        switch(value) {
            case SlatedbException.Transaction x -> {
                buf.putInt(1);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case SlatedbException.Closed x -> {
                buf.putInt(2);
                FfiConverterTypeCloseReason.INSTANCE.write(x.reason, buf);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case SlatedbException.Unavailable x -> {
                buf.putInt(3);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case SlatedbException.Invalid x -> {
                buf.putInt(4);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case SlatedbException.Data x -> {
                buf.putInt(5);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            case SlatedbException.Internal x -> {
                buf.putInt(6);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
        };
    }
}


