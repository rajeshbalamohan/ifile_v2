package ifile_v2;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.SecureRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.tez.runtime.library.common.ifile2.IFile2;
import org.apache.tez.runtime.library.common.ifile2.IFile2.KV_TRAIT;
import org.apache.tez.runtime.library.common.ifile2.IFile2.ReaderOptions;
import org.apache.tez.runtime.library.common.ifile2.IFile2.WriterOptions;
import org.apache.tez.runtime.library.common.ifile2.Reader;
import org.apache.tez.runtime.library.common.ifile2.Writer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class TestLegacyIFile {

  private static final String BASE_DIR = "target";

  private FileSystem fs;
  private Configuration conf;
  private DataInputBuffer key = new DataInputBuffer();
  private DataInputBuffer value = new DataInputBuffer();
  private CompressionCodecFactory codecFactory;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf).getRaw();
    codecFactory = new CompressionCodecFactory(new Configuration());
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
  }

  /**
   * Write a file with specific file size. Data will be random data.
   * 
   * 
   * @param options
   * @param numOfKeys
   * @throws IOException
   */
  public void createIFile(WriterOptions options, long numOfKeys)
      throws IOException {
    Writer writer = IFile2.createWriter(options);
    SecureRandom rnd = new SecureRandom();
    while (numOfKeys > 0) {
      String k = "Key_" + rnd.nextInt();
      String v = "Value_" + rnd.nextInt();

      key.reset(k.getBytes(), k.getBytes().length);
      value.reset(v.getBytes(), v.getBytes().length);
      writer.append(key, value);
      numOfKeys--;
    }
    writer.close();
  }

  /**
   * Write IFile by reading data from inputFile
   */
  public long createIFile(WriterOptions options, Path csvFile)
      throws IOException {
    Preconditions.checkArgument(fs.exists(csvFile));
    Writer writer = IFile2.createWriter(options);
    FSDataInputStream in = fs.open(csvFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    long recordCount = 0;
    while (reader.ready()) {
      String line = reader.readLine();
      String[] kv = line.split(",");
      if (kv.length != 2) {
        throw new IllegalArgumentException("Wrong line : " + line);
      }
      String k = kv[0];
      String v = kv[1];

      key.reset(k.getBytes(), k.getBytes().length);
      value.reset(v.getBytes(), v.getBytes().length);
      writer.append(key, value);
      recordCount++;
    }
    reader.close();
    writer.close();
    return recordCount;
  }

  /**
   * Read all records in a file
   * 
   * @param options
   * @return
   * @throws IOException
   */
  public long readIFile(ReaderOptions options) throws IOException {
    Reader reader = IFile2.createReader(options);
    long recordCount = 0;
    while (reader.nextRawKey(key)) {
      value = new DataInputBuffer();
      reader.nextRawValue(value);
      recordCount++;
    }
    reader.close();
    return recordCount;
  }

  @Test
  public void testMultiKVIFile() throws Exception {
    String inputfile = "store_sales_sample.csv";
    // String inputfile = "store_sales_60_l.csv";
    Path file = new Path(BASE_DIR, "multi_kv_no_compress.out");
    WriterOptions writeOptions = new WriterOptions();
    /*
     * org.apache.hadoop.io.compress.BZip2Codec,
     * org.apache.hadoop.io.compress.DefaultCodec
     * org.apache.hadoop.io.compress.DeflateCodec
     * org.apache.hadoop.io.compress.GzipCodec
     * org.apache.hadoop.io.compress.Lz4Codec
     * org.apache.hadoop.io.compress.SnappyCodec
     */
    conf.set("ifile.trait", KV_TRAIT.MULTI_KV.toString());
    writeOptions = new WriterOptions();
    writeOptions.setConf(conf).setFilePath(fs, file).setCodec(null)
      .setKVClasses(BytesWritable.class, BytesWritable.class).setRLE(false);
    fs.deleteOnExit(file);
    createIFile(writeOptions, new Path(".", inputfile));

    ReaderOptions readOptions = new ReaderOptions();
    readOptions.setBufferSize(4 * 1024).setFilePath(fs, file)
      .setLength(fs.getFileStatus(file).getLen()).setInputStream(fs.open(file))
      .setReadAhead(true, 4 * 1024);
    long readKeys = readIFile(readOptions);

    file = new Path(BASE_DIR, "multi_kv_default_compression.out");
    writeOptions = new WriterOptions();
    writeOptions.setConf(conf).setFilePath(fs, file)
      .setCodec(codecFactory.getCodecByName("Default"))
      .setKVClasses(BytesWritable.class, BytesWritable.class).setRLE(false);
    createIFile(writeOptions, new Path(".", inputfile));
  }

  @Test
  public void testLegacyIFile() throws Exception {
    String inputfile = "store_sales_sample.csv";
    // String inputfile = "store_sales_60_l.csv";
    Path file = new Path(BASE_DIR, "legacyfile_default_compression.out");
    WriterOptions writeOptions = new WriterOptions();
    /*
     * org.apache.hadoop.io.compress.BZip2Codec,
     * org.apache.hadoop.io.compress.DefaultCodec
     * org.apache.hadoop.io.compress.DeflateCodec
     * org.apache.hadoop.io.compress.GzipCodec
     * org.apache.hadoop.io.compress.Lz4Codec
     * org.apache.hadoop.io.compress.SnappyCodec
     */

    writeOptions.setConf(conf).setFilePath(fs, file)
      .setCodec(codecFactory.getCodecByName("Default"))
      .setKVClasses(BytesWritable.class, BytesWritable.class).setRLE(false);
    createIFile(writeOptions, new Path(".", inputfile));

    file = new Path(BASE_DIR, "legacyfile_default_compression_with_rle.out");
    writeOptions = new WriterOptions();
    writeOptions.setConf(conf).setFilePath(fs, file)
      .setCodec(codecFactory.getCodecByName("Default"))
      .setKVClasses(BytesWritable.class, BytesWritable.class).setRLE(true);
    createIFile(writeOptions, new Path(".", inputfile));

    file = new Path(BASE_DIR, "legacyfile_no_compress.out");
    writeOptions = new WriterOptions();
    writeOptions.setConf(conf).setFilePath(fs, file).setCodec(null)
      .setKVClasses(BytesWritable.class, BytesWritable.class).setRLE(false);
    long recordCount = createIFile(writeOptions, new Path(".", inputfile));

    ReaderOptions readOptions = new ReaderOptions();
    readOptions.setBufferSize(4 * 1024).setFilePath(fs, file)
      .setLength(fs.getFileStatus(file).getLen()).setInputStream(fs.open(file))
      .setReadAhead(true, 4 * 1024);
    long readKeys = readIFile(readOptions);
    assertEquals(recordCount, readKeys);
  }
}
