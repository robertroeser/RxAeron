/* Generated SBE (Simple Binary Encoding) message codec */
package io.reactivex.aeron.protocol;

import uk.co.real_logic.sbe.codec.java.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;

public class BinDataEncodingEncoder
{
    private MutableDirectBuffer buffer;
    private int offset;
    private int actingVersion;

    public BinDataEncodingEncoder wrap(final MutableDirectBuffer buffer, final int offset, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingVersion = actingVersion;
        return this;
    }

    public int size()
    {
        return -1;
    }

    public static short lengthNullValue()
    {
        return (short)255;
    }

    public static short lengthMinValue()
    {
        return (short)0;
    }

    public static short lengthMaxValue()
    {
        return (short)254;
    }
    public BinDataEncodingEncoder length(final short value)
    {
        CodecUtil.uint8Put(buffer, offset + 0, value);
        return this;
    }

    public static short varDataNullValue()
    {
        return (short)255;
    }

    public static short varDataMinValue()
    {
        return (short)0;
    }

    public static short varDataMaxValue()
    {
        return (short)254;
    }
}
