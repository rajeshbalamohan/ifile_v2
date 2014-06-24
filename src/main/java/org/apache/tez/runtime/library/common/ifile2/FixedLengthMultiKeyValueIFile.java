package org.apache.tez.runtime.library.common.ifile2;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BufferUtils;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.common.ifile2.IFile2.AbstractReader;
import org.apache.tez.runtime.library.common.ifile2.IFile2.AbstractWriter;
import org.apache.tez.runtime.library.common.ifile2.IFile2.KV_TRAIT;
import org.apache.tez.runtime.library.common.ifile2.IFile2.KeyState;
import org.apache.tez.runtime.library.common.ifile2.IFile2.ReaderOptions;
import org.apache.tez.runtime.library.common.ifile2.IFile2.WriterOptions;

/**
 * 
 * fixed k/v length file format. Here we dont need to write K/V lengths. Instead
 * of tryingout int,float and separate one for string etc, directly try out this
 * wherein users can specify the fixed lengths of key and value. In cases of
 * spaces, compressor should take care of reducing the sizes.
 *
 */
// TODO: Need to have others like Int, Float etc. But for quick testing, trying
// this out
// TODO: Work in progress. Not stable for testing.
public class FixedLengthMultiKeyValueIFile extends MultiKeyValueIFile {

  static class FixedLengthKVWriter extends AbstractWriter {

    public FixedLengthKVWriter(WriterOptions options) throws IOException {
      super(options, KV_TRAIT.FIXED_LENGTH_MULTI_KV);
      this.keyLength = options.fixedKeyLength;
      this.valueLength = options.fixedValueLength;
      // write lengths as a part of header detail out.writeInt(keyLength);
      out.writeInt(valueLength);
      decompressedBytesWritten += WritableUtils.getVIntSize(keyLength);
      decompressedBytesWritten += WritableUtils.getVIntSize(valueLength);
    }

    @Override
    public void append(Object key, Object value) throws IOException {
      // TODO: need to fill in this
      throw new IOException("Yet to implement this");
    }

    protected void validateKVLength(DataInputBuffer key, DataInputBuffer value)
        throws IOException {
      if (key.getLength() < keyLength) {
        throw new IOException("Expecting keyLen: " + keyLength + "; got "
            + key.getLength());
      }

      if (value.getLength() < valueLength) {
        throw new IOException("Expecting keyLen: " + valueLength + "; got "
            + value.getLength());
      }
    }

    @Override
    public void append(DataInputBuffer key, DataInputBuffer value)
        throws IOException {
      super.validateKVLength(key, value);

      // Lengths are already validated and all key lengths in this file are
      // same.
      boolean sameKey =
          (keyLength != 0) && (BufferUtils.compare(previous, key) == 0);

      if (sameKey) {
        out.write(value.getData(), value.getPosition(), valueLength);

        // Update bytes written
        decompressedBytesWritten += valueLength;
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(0 + valueLength);
        }
      } else {
        WritableUtils.writeVInt(out, V_END_MARKER);

        out.write(key.getData(), key.getPosition(), keyLength);
        out.write(value.getData(), value.getPosition(), valueLength);

        // Update bytes written
        decompressedBytesWritten +=
            WritableUtils.getVIntSize(V_END_MARKER) + keyLength + valueLength;
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(keyLength + valueLength);
        }

        BufferUtils.copy(key, previous);
      }
      ++numRecordsWritten;
    }
  }

  static class FixedLengthKVReader extends AbstractReader {

    private boolean canReadNextKey = true;
    private int currentLength;
    private int keyLength;
    private int valueLength;

    public FixedLengthKVReader(ReaderOptions options) throws IOException {
      super(options);
      this.keyLength = dataIn.readInt();
      this.valueLength = dataIn.readInt();
      bytesRead += WritableUtils.getVIntSize(keyLength);
      bytesRead += WritableUtils.getVIntSize(valueLength);
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
