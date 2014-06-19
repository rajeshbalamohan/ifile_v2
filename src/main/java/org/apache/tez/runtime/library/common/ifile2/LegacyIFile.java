package org.apache.tez.runtime.library.common.ifile2;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BufferUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.runtime.library.common.ifile2.IFile2.AbstractReader;
import org.apache.tez.runtime.library.common.ifile2.IFile2.AbstractWriter;
import org.apache.tez.runtime.library.common.ifile2.IFile2.KV_TRAIT;
import org.apache.tez.runtime.library.common.ifile2.IFile2.KeyState;
import org.apache.tez.runtime.library.common.ifile2.IFile2.ReaderOptions;
import org.apache.tez.runtime.library.common.ifile2.IFile2.WriterOptions;

/**
 *
 * Simple KV Reader/Writer based on KL,VL,K,V In addition to this, it writes the
 * header byte.
 *
 */
class LegacyIFile {
  static class KVWriter extends AbstractWriter {
    static Log LOG = LogFactory.getLog(LegacyIFile.KVWriter.class);

    public KVWriter(WriterOptions options) throws IOException {
      super(options, KV_TRAIT.KV);
    }

    @Override
    public void append(Object key, Object value) throws IOException {
      super.validateKVClass(key, value);

      boolean sameKey = false;

      // Append the 'key'
      keySerializer.serialize(key);
      int keyLength = buffer.getLength();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength
            + " for " + key);
      }

      if (rle && keyLength == previous.getLength()) {
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

      if (rle && sameKey) {
        WritableUtils.writeVInt(out, IFile2.RLE_MARKER); // Same key as previous
        WritableUtils.writeVInt(out, valueLength); // value length
        out.write(buffer.getData(), keyLength, buffer.getLength()); // only
                                                                    // the
                                                                    // value
        // Update bytes written
        decompressedBytesWritten +=
            0 + valueLength + WritableUtils.getVIntSize(IFile2.RLE_MARKER)
                + WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(0 + valueLength);
        }
      } else {
        // Write the record out
        WritableUtils.writeVInt(out, keyLength); // key length
        WritableUtils.writeVInt(out, valueLength); // value length
        out.write(buffer.getData(), 0, buffer.getLength()); // data
        // Update bytes written
        decompressedBytesWritten +=
            keyLength + valueLength + WritableUtils.getVIntSize(keyLength)
                + WritableUtils.getVIntSize(valueLength);
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

      if (rle && keyLength == previous.getLength()) {
        sameKey = (keyLength != 0) && (BufferUtils.compare(previous, key) == 0);
      }
      if (rle && sameKey) {
        WritableUtils.writeVInt(out, IFile2.RLE_MARKER);
        WritableUtils.writeVInt(out, valueLength);
        out.write(value.getData(), value.getPosition(), valueLength);

        // Update bytes written
        decompressedBytesWritten +=
            0 + valueLength + WritableUtils.getVIntSize(IFile2.RLE_MARKER)
                + WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(0 + valueLength);
        }
      } else {
        WritableUtils.writeVInt(out, keyLength);
        WritableUtils.writeVInt(out, valueLength);
        out.write(key.getData(), key.getPosition(), keyLength);
        out.write(value.getData(), value.getPosition(), valueLength);

        // Update bytes written
        decompressedBytesWritten +=
            keyLength + valueLength + WritableUtils.getVIntSize(keyLength)
                + WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(keyLength + valueLength);
        }

        BufferUtils.copy(key, previous);
      }
      ++numRecordsWritten;
    }

    @Override
    public void append(DataInputBuffer key, Iterator<byte[]> value)
        throws IOException {
      throw new IOException("Method not yet supported");
    }
  }

  /**
   * Reader class
   */
  static class KVReader extends AbstractReader {

    public KVReader(ReaderOptions options) throws IOException {
      super(options);
    }

    boolean positionToNextRecord(DataInput dIn) throws IOException {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }

      // Read key and value lengths
      prevKeyLength = currentKeyLength;
      currentKeyLength = WritableUtils.readVInt(dIn);
      currentValueLength = WritableUtils.readVInt(dIn);
      bytesRead +=
          WritableUtils.getVIntSize(currentKeyLength)
              + WritableUtils.getVIntSize(currentValueLength);

      // Check for EOF
      if (currentKeyLength == IFile2.EOF_MARKER
          && currentValueLength == IFile2.EOF_MARKER) {
        eof = true;
        return false;
      }

      // Sanity check
      if (currentKeyLength != IFile2.RLE_MARKER && currentKeyLength < 0) {
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
      if (currentKeyLength == IFile2.RLE_MARKER) {
        currentKeyLength = prevKeyLength;
        // no data to read
        key.reset(keyBytes, currentKeyLength);
        return KeyState.SAME_KEY;
      }
      if (keyBytes.length < currentKeyLength) {
        keyBytes = new byte[currentKeyLength << 1];
      }
      int i = readData(keyBytes, 0, currentKeyLength);
      if (i != currentKeyLength) {
        throw new IOException("Asked for " + currentKeyLength + " Got: " + i);
      }
      key.reset(keyBytes, currentKeyLength);
      bytesRead += currentKeyLength;
      return KeyState.NEW_KEY;
    }

    public void nextRawValue(DataInputBuffer value) throws IOException {
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