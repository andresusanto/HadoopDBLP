// XmlInputFormat
// (C) 2015 by alexholmes and andresusanto
// forked from alexholmes
// modified by andresusanto in order to make it work with DBLP


import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.slf4j.*;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {

  private static final Logger log =
      LoggerFactory.getLogger(XmlInputFormat.class);

  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    try {
      return new XmlRecordReader((FileSplit) split,
          context.getConfiguration());
    } catch (IOException ioe) {
      log.warn("Error while creating XmlRecordReader", ioe);
      return null;
    }
  }

  /**
   * XMLRecordReader class to read through a given xml document to
   * output xml blocks as records as specified
   * by the start tag and end tag
   */
  public static class XmlRecordReader
      extends RecordReader<LongWritable, Text> {

    private final byte[] startTag;
    private final byte[] endTag;
    private final long start;
    private final long end;
    private final FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable currentKey;
    private Text currentValue;

    public XmlRecordReader(FileSplit split, Configuration conf)
        throws IOException {
      startTag = conf.get(START_TAG_KEY).getBytes("UTF-8");
      endTag = conf.get(END_TAG_KEY).getBytes("UTF-8");

      // open the file and seek to the start of the split
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(conf);
      fsin = fs.open(split.getPath());
      fsin.seek(start);
    }

    private boolean next(LongWritable key, Text value)
        throws IOException {
      if (fsin.getPos() < end && readUntilMatch(startTag, false)) {
        try {
          //buffer.write(startTag);
          if (readUntilMatch(endTag, true)) {
            key.set(fsin.getPos());
            value.set(buffer.getData(), 0, buffer.getLength());
			//throw new IOException(new String(buffer.getData(), StandardCharsets.UTF_8));
            return true;
          }
        } finally {
          buffer.reset();
        }
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      fsin.close();
    }

    @Override
    public float getProgress() throws IOException {
      return (fsin.getPos() - start) / (float) (end - start);
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
		StringBuilder sb = new StringBuilder();
		String allMatchs = new String(match, StandardCharsets.UTF_8);
		List matches = Arrays.asList(allMatchs.split(","));
		
		
		int state = 0;
		// STATE menjelaskan posisi/status dari LOOP
		// STATE == 0, loop baru dimulai, cari '<' sampai ketemu
		// STATE == 1, loop sudah menemukan '<', lanjutkan membaca sampai menemukan spasi. Lalu periksa apakah ada yg match
		//				jika ada yg match, lanjut ke state == 2, jika tidak loop sampai ketemu '>', lalu reset state menjadi 0
		// STATE == 2, ada yang match, lakukan loop sampai ketemu '>' lalu return true
			
		while (true) {
			int b = fsin.read();
			char bc = (char) b;
			
			// penanganan karakter bhs jerman dsb
			if (bc == '&' || bc == ';') {
				b = 32;
				bc = ' ';
			}
			
			// buffer data yang diperlukan
			if (withinBlock) {
				buffer.write(b);
			}
					
			switch(state){
				case 0:
					if (b == -1) {
						return false;
					}
					
					if (bc == '<'){
						state = 1;
					}else if (!withinBlock && fsin.getPos() >= end) {
						return false;
					}
					
					break;
				case 1:
					if (bc != ' ' && bc != '>'){
						sb.append(bc);
					}else{
						String tag = sb.toString();
						if (matches.contains(tag)){		
							if (bc == '>'){
								return true;
							}
							state = 2;
						}else{
							state = 0;
						}
						sb.setLength(0);
						
						if (!withinBlock && state == 2){
							buffer.write('<');
							buffer.write(tag.getBytes());
							buffer.write('>');
						}
						
						
						//throw new IOException("COMPARE " + tag + " IN " + matches.size());
					}
					break;
				case 2:
					if (bc == '>'){
						return true;
					}
					break;
			}
			
			
		}
		
    }

    @Override
    public LongWritable getCurrentKey()
        throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public Text getCurrentValue()
        throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public void initialize(InputSplit split,
                           TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException {
      currentKey = new LongWritable();
      currentValue = new Text();
      return next(currentKey, currentValue);
    }
  }
}