package org.apache.tez.runtime.library.common.ifile2;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;

public interface Reader {
  public long getLength();

  public long getPosition() throws IOException;

  public boolean nextRawKey(DataInputBuffer key) throws IOException;

  public void nextRawValue(DataInputBuffer value) throws IOException;

  public void close() throws IOException;
}