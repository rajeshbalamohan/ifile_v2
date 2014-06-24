package org.apache.tez.runtime.library.common.ifile2;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.common.sort.impl.IFileInputStream;
import org.apache.tez.runtime.library.common.sort.impl.IFileOutputStream;

import com.google.common.base.Preconditions;

/**
 * <pre>
 * IFile 2 for benchmarking.
 * - Supports both legacy file as well as MultiKV format.
 * </pre>
 *
 */
public class IFile2 {
  static final Log LOG = LogFactory.getLog(IFile2.class);

  public static final int EOF_MARKER = -1; // End of File Marker
  public static final int RLE_MARKER = -2; // Repeat same key marker
  public static final DataInputBuffer REPEAT_KEY = new DataInputBuffer();

  public static enum KV_TRAIT {
    KV("KV", 1), MULTI_KV("MULTI_KV", 2), FIXED_LENGTH_MULTI_KV(
        "FIXED_LENGTH_MULTI_KV", 3);

    private final String traitName;
    private final int traitNum;

    private KV_TRAIT(String traitName, int traitNum) {
      this.traitName = traitName;
      this.traitNum = traitNum;
    }

    public static KV_TRAIT getTrait(int traitNum) {
      for (KV_TRAIT trait : values())
        if (trait.traitNum == traitNum)
          return trait;
      throw new IllegalArgumentException();
    }
  }

  public static Writer createWriter(Configuration conf, FileSystem fs,
      Path file, Class keyClass, Class valueClass, CompressionCodec codec,
      TezCounter writesCounter, TezCounter serializedBytesCounter)
      throws IOException {
    WriterOptions options = new WriterOptions();
    options.setConf(conf).setCodec(codec).setFilePath(fs, file)
      .setCounters(writesCounter, serializedBytesCounter);
    return createWriter(options);
  }

  public static Writer createWriter(WriterOptions writerOptions)
      throws IOException {
    Preconditions.checkArgument((writerOptions != null),
      "Writer options can not be null");
    Preconditions.checkArgument((writerOptions.conf != null),
      "conf can not be null");

    KV_TRAIT traits =
        KV_TRAIT.valueOf(writerOptions.conf.get("ifile.trait",
          KV_TRAIT.KV.toString()));

    switch (traits) {
    // TODO: Additional custom impls possible
    case KV:
      return new LegacyIFile.KVWriter(writerOptions);
    case MULTI_KV:
      return new MultiKeyValueIFile.KVWriter(writerOptions);
    default:
      throw new IllegalArgumentException("Illegal arguement");
    }
  }

  public static Reader createReader(InputStream in, long length,
      CompressionCodec codec, TezCounter readsCounter,
      TezCounter bytesReadCounter, boolean readAhead, int readAheadLength,
      int bufferSize) throws IOException {
    ReaderOptions options = new ReaderOptions();
    options.setBufferSize(bufferSize).setReadAhead(readAhead, readAheadLength)
      .setCounters(readsCounter, bytesReadCounter).setCodec(codec)
      .setLength(length).setInputStream(in);
    return createReader(options);
  }

  public static Reader createReader(ReaderOptions readerOptions)
      throws IOException {
    Preconditions.checkArgument((readerOptions != null),
      "Reader options can not be null");
    KV_TRAIT trait =
        getTrait(readerOptions.fs, readerOptions.filePath, readerOptions.codec);
    switch (trait) {
    // TODO: Additional custom impls possible
    case KV:
      return new LegacyIFile.KVReader(readerOptions);
    case MULTI_KV:
      return new MultiKeyValueIFile.KVReader(readerOptions);
    default:
      throw new IllegalArgumentException("Illegal arguement");
    }
  }

  public static KV_TRAIT getTrait(FileSystem fs, Path filePath,
      CompressionCodec codec) throws IOException {
    InputStream in = fs.open(filePath);
    if (codec != null) {
      Decompressor decompressor = CodecPool.getDecompressor(codec);
      if (decompressor != null) {
        in = codec.createInputStream(in, decompressor);
      } else {
        LOG.warn("Could not obtain decompressor from CodecPool");
      }
    }
    DataInputStream din = new DataInputStream(in);
    int traitNum = din.readInt();
    in.close();
    KV_TRAIT trait = KV_TRAIT.getTrait(traitNum);
    return trait;
  }

  /**
   * Writer option builder
   * 
   */
  public static class WriterOptions {
    Configuration conf;
    FileSystem fs;
    Path filePath;
    Class keyClass;
    Class valueClass;
    CompressionCodec codec;
    TezCounter writesCounter;
    TezCounter serializedBytesCounter;
    boolean rle;
    int fixedKeyLength;
    int fixedValueLength;

    public WriterOptions setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public WriterOptions setFilePath(FileSystem fs, Path filePath) {
      this.fs = fs;
      this.filePath = filePath;
      return this;
    }

    public WriterOptions setKVClasses(Class keyClass, Class valueClass) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      return this;
    }

    public WriterOptions setCodec(CompressionCodec codec) {
      this.codec = codec;
      return this;
    }

    public WriterOptions setCounters(TezCounter writesCounter,
        TezCounter serializedBytesCounter) {
      this.writesCounter = writesCounter;
      this.serializedBytesCounter = serializedBytesCounter;
      return this;
    }

    public WriterOptions setRLE(boolean rle) {

      this.rle = rle;
      return this;
    }

    public boolean validate() {
      // TODO: do some basic validations here
      return true;
    }

    public WriterOptions
        setFixedLength(int fixedKeyLength, int fixedValueLength) {
      this.fixedKeyLength = fixedKeyLength;
      this.fixedValueLength = fixedValueLength;
      return this;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("codec=")
        .append((codec == null) ? "" : codec.getCompressorType()).append("; ")
        .append("rle=").append(rle).append("; file=").append(filePath);
      return sb.toString();
    }
  }

  public static abstract class AbstractWriter implements Writer {
    protected FSDataOutputStream out;
    protected boolean ownOutputStream = false;
    protected long start = 0;
    protected final FSDataOutputStream rawOut;
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    protected CompressionOutputStream compressedOut;
    protected Compressor compressor;
    protected boolean compressOutput = false;

    protected long decompressedBytesWritten = 0;
    protected long compressedBytesWritten = 0;

    // Count records written to disk
    protected long numRecordsWritten = 0;
    protected TezCounter writtenRecordsCounter;
    protected TezCounter serializedUncompressedBytes;

    protected IFileOutputStream checksumOut;

    protected Class keyClass;
    protected Class valueClass;
    protected Serializer keySerializer;
    protected Serializer valueSerializer;

    protected DataOutputBuffer buffer = new DataOutputBuffer();
    protected DataOutputBuffer previous = new DataOutputBuffer();

    protected Configuration conf;
    protected int keyLength;
    protected int valueLength;

    protected boolean rle;
    protected KV_TRAIT trait;

    public AbstractWriter(WriterOptions options, KV_TRAIT trait)
        throws IOException {
      this(options);
      out.writeInt(trait.traitNum);
      decompressedBytesWritten++;
      if (serializedUncompressedBytes != null) {
        serializedUncompressedBytes.increment(1);
      }
    }

    public AbstractWriter(WriterOptions options) throws IOException {
      this.writtenRecordsCounter = options.writesCounter;
      this.serializedUncompressedBytes = options.serializedBytesCounter;
      this.out = options.fs.create(options.filePath);
      this.rawOut = out;
      this.checksumOut = new IFileOutputStream(out);
      this.rle = options.rle;
      this.start = this.rawOut.getPos();
      if (options.codec != null) {
        this.compressor = CodecPool.getCompressor(options.codec);
        if (this.compressor != null) {
          this.compressor.reset();
          this.compressedOut =
              options.codec.createOutputStream(checksumOut, compressor);
          this.out = new FSDataOutputStream(this.compressedOut, null);
          this.compressOutput = true;
        } else {
          LOG.warn("Could not obtain compressor from CodecPool");
          this.out = new FSDataOutputStream(checksumOut, null);
        }
      } else {
        this.out = new FSDataOutputStream(checksumOut, null);
      }

      this.keyClass = options.keyClass;
      this.valueClass = options.valueClass;

      if (keyClass != null) {
        this.conf = options.conf;
        SerializationFactory serializationFactory =
            new SerializationFactory(conf);
        this.keySerializer = serializationFactory.getSerializer(keyClass);
        this.keySerializer.open(buffer);
        this.valueSerializer = serializationFactory.getSerializer(valueClass);
        this.valueSerializer.open(buffer);
      }
      ownOutputStream = true;
    }

    public void close() throws IOException {
      if (closed.getAndSet(true)) {
        throw new IOException("Writer was already closed earlier");
      }

      // When IFile writer is created by BackupStore, we do not have
      // Key and Value classes set. So, check before closing the
      // serializers
      if (keyClass != null) {
        keySerializer.close();
        valueSerializer.close();
      }

      // Write EOF_MARKER for key/value length
      WritableUtils.writeVInt(out, EOF_MARKER);
      WritableUtils.writeVInt(out, EOF_MARKER);
      decompressedBytesWritten += 2 * WritableUtils.getVIntSize(EOF_MARKER);

      // Flush the stream
      out.flush();

      if (compressOutput) {
        // Flush
        compressedOut.finish();
        compressedOut.resetState();
      }

      // Close the underlying stream iff we own it...
      if (ownOutputStream) {
        out.close();
      } else {
        // Write the checksum
        checksumOut.finish();
      }

      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // Return back the compressor
        CodecPool.returnCompressor(compressor);
        compressor = null;
      }

      out = null;
      if (writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
    }

    @Override
    public DataOutputStream getOutputStream() {
      return out;
    }

    @Override
    public long getRawLength() {
      return decompressedBytesWritten;
    }

    @Override
    public long getCompressedLength() {
      return compressedBytesWritten;
    }

    @Override
    public void append(DataInputBuffer key, Iterator<byte[]> valItr)
        throws IOException {
      Preconditions.checkArgument((valItr != null),
        " value iterator can't be null");
      DataInputBuffer value = new DataInputBuffer();
      while (valItr.hasNext()) {
        byte[] val = valItr.next();
        value.reset(val, val.length);
        // rest is taken care by downstream
        append(key, value);
      }
    }

    protected void validateKVClass(Object key, Object value) throws IOException {
      if (key.getClass() != keyClass) {
        throw new IOException("wrong key class: " + key.getClass() + " is not "
            + keyClass);
      }

      if (value.getClass() != valueClass) {
        throw new IOException("wrong value class: " + value.getClass()
            + " is not " + valueClass);
      }
    }

    protected void validateKVLength(DataInputBuffer key, DataInputBuffer value)
        throws IOException {
      keyLength = key.getLength() - key.getPosition();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength
            + " for " + key);
      }

      valueLength = value.getLength() - value.getPosition();
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: "
            + valueLength + " for " + value);
      }
    }
  }

  public static class ReaderOptions {
    private FileSystem fs;
    private Path filePath;
    private CompressionCodec codec;
    private TezCounter readsCounter;
    private TezCounter bytesReadCounter;
    private boolean readAhead;
    private int readAheadLength;
    private int bufferSize;
    private InputStream in;
    private long length;

    public ReaderOptions setInputStream(InputStream in) {
      this.in = in;
      return this;
    }

    public ReaderOptions setLength(long length) {
      this.length = length;
      return this;
    }

    public ReaderOptions setFilePath(FileSystem fs, Path filePath) {
      this.fs = fs;
      this.filePath = filePath;
      return this;
    }

    public ReaderOptions setCounters(TezCounter readsCounter,
        TezCounter bytesReadCounter) {
      this.readsCounter = readsCounter;
      this.bytesReadCounter = bytesReadCounter;
      return this;
    }

    public ReaderOptions setCodec(CompressionCodec codec) {
      this.codec = codec;
      return this;
    }

    public ReaderOptions setReadAhead(boolean readAhead, int readAheadLength) {
      this.readAhead = readAhead;
      this.readAheadLength = readAheadLength;
      return this;
    }

    public ReaderOptions setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public boolean validate() {
      // TODO: fix this.
      return false;
    }
  }

  public enum KeyState {
    NO_KEY, NEW_KEY, SAME_KEY
  };

  public static abstract class AbstractReader implements Reader {

    private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;

    // Count records read from disk
    protected long numRecordsRead = 0;
    private final TezCounter readRecordsCounter;
    private final TezCounter bytesReadCounter;

    InputStream in; // Possibly decompressed stream that we read
    Decompressor decompressor;
    public long bytesRead = 0;
    protected final long fileLength;
    protected boolean eof = false;
    final IFileInputStream checksumIn;

    protected byte[] buffer = null;
    protected int bufferSize = DEFAULT_BUFFER_SIZE;
    protected DataInputStream dataIn;

    protected int recNo = 1;
    protected int prevKeyLength;
    protected int currentKeyLength;
    protected int currentValueLength;
    byte keyBytes[] = new byte[0];

    long startPos;

    public AbstractReader(ReaderOptions options) throws IOException {
      this.readRecordsCounter = options.readsCounter;
      this.bytesReadCounter = options.bytesReadCounter;
      this.in = options.fs.open(options.filePath);
      this.fileLength = options.fs.getFileStatus(options.filePath).getLen();
      checksumIn =
          new IFileInputStream(in, fileLength, options.readAhead,
            options.readAheadLength);
      if (options.codec != null) {
        decompressor = CodecPool.getDecompressor(options.codec);
        if (decompressor != null) {
          this.in = options.codec.createInputStream(checksumIn, decompressor);
        } else {
          LOG.warn("Could not obtain decompressor from CodecPool");
          this.in = checksumIn;
        }
      } else {
        this.in = checksumIn;
      }
      this.dataIn = new DataInputStream(this.in);
      // TODO: Read the first int as it contains the trait information. Process
      // later.
      int trait = dataIn.readInt();

      startPos = checksumIn.getPosition();
      bytesRead++;

      if (bufferSize != -1) {
        this.bufferSize = bufferSize;
      }
    }

    @Override
    public long getLength() {
      return fileLength - checksumIn.getSize();
    }

    @Override
    public long getPosition() throws IOException {
      return checksumIn.getPosition();
    }

    /**
     * Read upto len bytes into buf starting at offset off.
     * 
     * @param buf
     *          buffer
     * @param off
     *          offset
     * @param len
     *          length of buffer
     * @return the no. of bytes read
     * @throws IOException
     */
    protected int readData(byte[] buf, int off, int len) throws IOException {
      int bytesRead = 0;
      while (bytesRead < len) {
        int n =
            IOUtils.wrappedReadForCompressedData(in, buf, off + bytesRead, len
                - bytesRead);
        if (n < 0) {
          return bytesRead;
        }
        bytesRead += n;
      }
      return len;
    }

    public void close() throws IOException {
      // Close the underlying stream
      in.close();

      // Release the buffer
      dataIn = null;
      buffer = null;
      if (readRecordsCounter != null) {
        readRecordsCounter.increment(numRecordsRead);
      }

      if (bytesReadCounter != null) {
        bytesReadCounter.increment(checksumIn.getPosition() - startPos
            + checksumIn.getSize());
      }

      // Return the decompressor
      if (decompressor != null) {
        decompressor.reset();
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }

    abstract boolean positionToNextRecord(DataInput dIn) throws IOException;
  }
}
