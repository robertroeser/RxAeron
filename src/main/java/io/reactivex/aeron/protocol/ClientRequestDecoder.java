/* Generated SBE (Simple Binary Encoding) message codec */
package io.reactivex.aeron.protocol;

import uk.co.real_logic.sbe.codec.java.*;
import uk.co.real_logic.agrona.DirectBuffer;

public class ClientRequestDecoder
{
    public static final int BLOCK_LENGTH = 16;
    public static final int TEMPLATE_ID = 3;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final ClientRequestDecoder parentMessage = this;
    private DirectBuffer buffer;
    protected int offset;
    protected int limit;
    protected int actingBlockLength;
    protected int actingVersion;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public int offset()
    {
        return offset;
    }

    public ClientRequestDecoder wrap(
        final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

        return this;
    }

    public int size()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        buffer.checkLimit(limit);
        this.limit = limit;
    }

    public static int connectionIdId()
    {
        return 1;
    }

    public static String connectionIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long connectionIdNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long connectionIdMinValue()
    {
        return 0x0L;
    }

    public static long connectionIdMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long connectionId()
    {
        return CodecUtil.uint64Get(buffer, offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
    }


    public static int transactionIdId()
    {
        return 2;
    }

    public static String transactionIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long transactionIdNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long transactionIdMinValue()
    {
        return 0x0L;
    }

    public static long transactionIdMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long transactionId()
    {
        return CodecUtil.uint64Get(buffer, offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN);
    }


    public static int payloadId()
    {
        return 3;
    }

    public static String payloadCharacterEncoding()
    {
        return "UTF-8";
    }

    public static String payloadMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int payloadHeaderSize()
    {
        return 1;
    }

    public int payloadLength()
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        buffer.checkLimit(limit + sizeOfLengthField);

        return CodecUtil.uint8Get(buffer, limit);
    }

    public int getPayload(final uk.co.real_logic.agrona.MutableDirectBuffer dst, final int dstOffset, final int length)
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        buffer.checkLimit(limit + sizeOfLengthField);
        final int dataLength = CodecUtil.uint8Get(buffer, limit);
        final int bytesCopied = Math.min(length, dataLength);
        limit(limit + sizeOfLengthField + dataLength);
        buffer.getBytes(limit + sizeOfLengthField, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int getPayload(final byte[] dst, final int dstOffset, final int length)
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        buffer.checkLimit(limit + sizeOfLengthField);
        final int dataLength = CodecUtil.uint8Get(buffer, limit);
        final int bytesCopied = Math.min(length, dataLength);
        limit(limit + sizeOfLengthField + dataLength);
        buffer.getBytes(limit + sizeOfLengthField, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public String payload()
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        buffer.checkLimit(limit + sizeOfLengthField);
        final int dataLength = CodecUtil.uint8Get(buffer, limit);
        limit(limit + sizeOfLengthField + dataLength);
        final byte[] tmp = new byte[dataLength];
        buffer.getBytes(limit + sizeOfLengthField, tmp, 0, dataLength);

        final String value;
        try
        {
            value = new String(tmp, "UTF-8");
        }
        catch (final java.io.UnsupportedEncodingException ex)
        {
            throw new RuntimeException(ex);
        }

        return value;
    }
}
