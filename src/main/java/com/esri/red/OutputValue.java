package com.esri.red;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 */
public final class OutputValue implements Writable
{
    public byte index;
    public byte[] bytes;

    public OutputValue()
    {
    }

    public OutputValue(
            final byte index,
            final byte[] bytes
    )
    {
        this.index = index;
        this.bytes = bytes;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        dataOutput.writeByte(index);
        dataOutput.writeInt(bytes.length);
        dataOutput.write(bytes);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        index = dataInput.readByte();
        final int length = dataInput.readInt();
        bytes = new byte[length];
        dataInput.readFully(bytes);
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof OutputValue))
        {
            return false;
        }

        final OutputValue outputValue = (OutputValue) o;

        if (index != outputValue.index)
        {
            return false;
        }
        if (!Arrays.equals(bytes, outputValue.bytes))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) index;
        result = 31 * result + Arrays.hashCode(bytes);
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuffer sb = new StringBuffer("OutputValue{");
        sb.append("bytes=");
        if (bytes == null)
        {
            sb.append("null");
        }
        else
        {
            sb.append('[');
            for (int i = 0; i < bytes.length; ++i)
            {
                sb.append(i == 0 ? "" : ", ").append(bytes[i]);
            }
            sb.append(']');
        }
        sb.append(", index=").append(index);
        sb.append('}');
        return sb.toString();
    }
}
