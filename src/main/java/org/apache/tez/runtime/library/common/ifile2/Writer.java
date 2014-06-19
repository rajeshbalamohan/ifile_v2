package org.apache.tez.runtime.library.common.ifile2;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;

public interface Writer {

  public void append(Object key, Object value) throws IOException;

  public void append(DataInputBuffer key, DataInputBuffer value)
      throws IOException;

  /**
   * 
   * @param key
   * @param valItr
   * @throws IOException
   */
  public void append(DataInputBuffer key, Iterator<byte[]> valItr)
      throws IOException;

  public DataOutputStream getOutputStream();

  public long getRawLength();

  public long getCompressedLength();

  public void close() throws IOException;
}