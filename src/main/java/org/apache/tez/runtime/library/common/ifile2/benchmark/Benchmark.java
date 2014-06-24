package org.apache.tez.runtime.library.common.ifile2.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.tez.runtime.library.common.ifile2.IFile2;
import org.apache.tez.runtime.library.common.ifile2.Writer;
import org.apache.tez.runtime.library.common.ifile2.IFile2.KV_TRAIT;
import org.apache.tez.runtime.library.common.ifile2.IFile2.WriterOptions;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

/**
 * For benchmarking KV, MultiKV with different options
 *
 */
public class Benchmark {

  private FileSystem fs;
  private Configuration conf;
  private DataInputBuffer key = new DataInputBuffer();
  private DataInputBuffer value = new DataInputBuffer();
  private CompressionCodecFactory codecFactory;

  private Path inputCSVFile;

  public Benchmark(Path inputCSVFile) throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf).getRaw();
    codecFactory = new CompressionCodecFactory(new Configuration());
    this.inputCSVFile = inputCSVFile;
    Preconditions.checkArgument(fs.exists(inputCSVFile),
      "Please provide a valid file (CSV) to read");
  }

  public void cleanup() throws IOException {
    if (fs != null) {
      fs.close();
    }
  }

  /**
   * Write IFile by reading data from inputFile
   */
  private void createIFile(WriterOptions options, KV_TRAIT trait)
      throws IOException {
    conf.set("ifile.trait", trait.toString());
    Writer writer = IFile2.createWriter(options);
    FSDataInputStream in = fs.open(inputCSVFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    while (reader.ready()) {
      String line = reader.readLine();
      String[] kv = line.split(",");
      Iterator<String> it =
          Splitter.on(",").omitEmptyStrings().split(line).iterator();
      String k = it.next();
      String v = it.next();

      key.reset(k.getBytes(), k.getBytes().length);
      value.reset(v.getBytes(), v.getBytes().length);
      writer.append(key, value);
    }
    reader.close();
    writer.close();
  }

  /**
   * Just a placeholder for storing results.
   */
  static class Result {
    String info;
    WriterOptions option;
    long size;

    public Result(String info, WriterOptions option, long size) {
      this.info = info;
      this.option = option;
      this.size = size;
    }

    public String toString() {
      return info + " " + option.toString() + " " + size;
    }
  }

  public void runBenchmark() throws IOException {
    System.out.println();
    System.out.println();
    System.out.println();
    System.out.println();

    /*
     * org.apache.hadoop.io.compress.BZip2Codec,
     * org.apache.hadoop.io.compress.DefaultCodec
     * org.apache.hadoop.io.compress.DeflateCodec
     * org.apache.hadoop.io.compress.GzipCodec
     * org.apache.hadoop.io.compress.Lz4Codec
     * org.apache.hadoop.io.compress.SnappyCodec
     */
    Configuration conf = new Configuration();
    List<Class<? extends CompressionCodec>> codecList =
        codecFactory.getCodecClasses(conf);

    EnumSet<KV_TRAIT> traitSet = EnumSet.of(KV_TRAIT.KV, KV_TRAIT.MULTI_KV);
    for (KV_TRAIT trait : traitSet) {
      for (Class<? extends CompressionCodec> codec : codecList) {
        try {
        String fileName = "result_" + trait + "_" + codec + ".out";

        Path file = new Path(".", fileName);
        WriterOptions writeOptions = new WriterOptions();

        writeOptions.setConf(conf).setFilePath(fs, file)
          .setCodec(codecFactory.getCodecByClassName(codec.getName()))
          .setRLE(false);
        createIFile(writeOptions, trait);
        Result rs =
            new Result(fileName, writeOptions, fs.getFileStatus(file).getLen());
        System.out.println(rs);

        // with RLE
        fileName = "result_" + trait + "_" + codec + "_rle.out";
        writeOptions.setRLE(true);
        createIFile(writeOptions, trait);
        rs =
            new Result(fileName, writeOptions, fs.getFileStatus(file).getLen());
        System.out.println(rs);
        } catch(Throwable t) {
          t.printStackTrace();
          //proceed to the next benchmark.  Quite possible that codec jars aren't available
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    Preconditions.checkArgument((args.length == 1),
      "Please enter valid filename (csv) to read");
    Benchmark benchmark = new Benchmark(new Path(args[0]));
    benchmark.runBenchmark();
  }
}
