package org.apache.tez.runtime.library.common.ifile2;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.BufferUtils;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.runtime.library.common.ifile2.IFile2.AbstractReader;
import org.apache.tez.runtime.library.common.ifile2.IFile2.AbstractWriter;
import org.apache.tez.runtime.library.common.ifile2.IFile2.KV_TRAIT;
import org.apache.tez.runtime.library.common.ifile2.IFile2.KeyState;
import org.apache.tez.runtime.library.common.ifile2.IFile2.ReaderOptions;
import org.apache.tez.runtime.library.common.ifile2.IFile2.WriterOptions;

/**
 * <pre>
 * Multi key-value reader/writer
 * e.g: 
 * KL,K,VL,V,V_END_MARKER
 * KL1,K1,VL1,V1,VL2,V2...VLn,Vn..V_END_MARKER,KL2,K2..
 * </pre>
 */
public class MultiKeyValueIFile {
  public static final int V_END_MARKER = -3;

  /**
   * <pre>
   * Multi key-value Writer
   * e.g: 
   * KL,K,VL,V,V_END_MARKER
   * KL1,K1,VL1,V1,VL2,V2...VLn,Vn..V_END_MARKER,KL2,K2..
   * </pre>
   */
  static class KVWriter extends AbstractWriter {
    public KVWriter(WriterOptions options) throws IOException {
      super(options, KV_TRAIT.MULTI_KV);
    }

    @Override
    public void append(Object key, Object value) throws IOException {
      boolean sameKey = false;

      // Append the 'key'
      keySerializer.serialize(key);
      int keyLength = buffer.getLength();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength
            + " for " + key);
      }

      if (keyLength == previous.getLength()) {
        sameKey = (BufferUtils.compare(previous, buffer) == 0);
      }

      if (!sameKey) {
        BufferUtils.copy(buffer, previous);
      }

      // Append the 'value'
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: "
            + valueLength + " for " + value);
      }

      if (sameKey) {
        WritableUtils.writeVInt(out, valueLength); // value length
        out.write(buffer.getData(), keyLength, buffer.getLength()); // only
                                                                    // the
                                                                    // value
        // Update bytes written
        decompressedBytesWritten +=
            0 + valueLength + WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(0 + valueLength);
        }
      } else {
        // Write the record out
        WritableUtils.writeVInt(out, V_END_MARKER);

        WritableUtils.writeVInt(out, keyLength); // key length
        out.write(buffer.getData(), 0, keyLength);
        WritableUtils.writeVInt(out, valueLength); // value length
        out.write(buffer.getData(), keyLength, buffer.getLength());
        // out.write(buffer.getData(), 0, buffer.getLength()); // data
        // Update bytes written
        decompressedBytesWritten +=
            WritableUtils.getVIntSize(V_END_MARKER)
                + WritableUtils.getVIntSize(keyLength) + keyLength
                + +WritableUtils.getVIntSize(valueLength) + valueLength;
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(keyLength + valueLength);
        }
      }

      // Reset
      buffer.reset();

      ++numRecordsWritten;
    }

    @Override
    public void append(DataInputBuffer key, DataInputBuffer value)
        throws IOException {
      super.validateKVLength(key, value);

      int valueLength = value.getLength() - value.getPosition();
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: "
            + valueLength + " for " + value);
      }

      boolean sameKey = false;

      if (keyLength == previous.getLength()) {
        sameKey = (keyLength != 0) && (BufferUtils.compare(previous, key) == 0);
      }
      if (sameKey) {
        WritableUtils.writeVInt(out, valueLength);
        out.write(value.getData(), value.getPosition(), valueLength);

        // Update bytes written
        decompressedBytesWritten +=
            0 + valueLength + WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(0 + valueLength);
        }
      } else {
        WritableUtils.writeVInt(out, V_END_MARKER);
        WritableUtils.writeVInt(out, keyLength);
        out.write(key.getData(), key.getPosition(), keyLength);

        WritableUtils.writeVInt(out, valueLength);
        out.write(value.getData(), value.getPosition(), valueLength);

        // Update bytes written
        decompressedBytesWritten +=
            WritableUtils.getVIntSize(V_END_MARKER) + keyLength
                + WritableUtils.getVIntSize(keyLength) + valueLength
                + +WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(keyLength + valueLength);
        }

        BufferUtils.copy(key, previous);
      }
      ++numRecordsWritten;
    }
  }

  /**
   * Reader class
   */
  static class KVReader extends AbstractReader {

    private boolean canReadNextKey = true;
    private int currentLength;

    public KVReader(ReaderOptions options) throws IOException {
      super(options);
    }

    boolean positionToNextRecord(DataInput dIn) throws IOException {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }

      currentLength = WritableUtils.readVInt(dataIn);
      bytesRead += WritableUtils.getVIntSize(currentLength);

      canReadNextKey = (currentLength == V_END_MARKER);
      if (currentLength == IFile2.EOF_MARKER) {
        eof = true;
        return false;
      }

      // Sanity check
      if (currentKeyLength != V_END_MARKER && currentKeyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: "
            + currentKeyLength);
      }
      if (currentValueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: "
            + currentValueLength);
      }

      return true;
    }

    public final boolean nextRawKey(DataInputBuffer key) throws IOException {
      return readRawKey(key) != KeyState.NO_KEY;
    }

    public KeyState readRawKey(DataInputBuffer key) throws IOException {
      if (!positionToNextRecord(dataIn)) {
        if (IFile2.LOG.isDebugEnabled()) {
          IFile2.LOG.debug("currentKeyLength=" + currentKeyLength
              + ", currentValueLength=" + currentValueLength + ", bytesRead="
              + bytesRead + ", length=" + fileLength);
        }
        return KeyState.NO_KEY;
      }
      if (!canReadNextKey) {
        // No need to read..its the same key.
        key.reset(keyBytes, currentKeyLength);
        currentValueLength = currentLength;
        return KeyState.SAME_KEY;
      }
      currentKeyLength = WritableUtils.readVInt(dataIn);
      bytesRead += WritableUtils.getVIntSize(currentKeyLength);
      keyBytes = new byte[currentKeyLength << 1];
      int i = readData(keyBytes, 0, currentKeyLength);
      if (i != currentKeyLength) {
        throw new IOException("Asked for " + currentKeyLength + " Got: " + i);
      }
      key.reset(keyBytes, currentKeyLength);
      bytesRead += currentKeyLength;

      return KeyState.NEW_KEY;
    }

    public void nextRawValue(DataInputBuffer value) throws IOException {
      // If its the same key as before, length field read by
      // positionToNextRecord would be the value length
      if (canReadNextKey) {
        currentValueLength = WritableUtils.readVInt(dataIn);
        bytesRead += WritableUtils.getVIntSize(currentValueLength);
      }
      final byte[] valBytes =
          ((value.getData().length < currentValueLength) || (value.getData() == keyBytes))
              ? new byte[currentValueLength << 1] : value.getData();
      int i = readData(valBytes, 0, currentValueLength);
      if (i != currentValueLength) {
        throw new IOException("Asked for " + currentValueLength + " Got: " + i);
      }
      value.reset(valBytes, currentValueLength);

      // Record the bytes read
      bytesRead += currentValueLength;
      ++recNo;
      ++numRecordsRead;
    }
  }
}
