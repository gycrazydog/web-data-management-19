package org.myorg;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
//import java.io.IOException;
 import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.*;
import org.jdom2.input.SAXBuilder;

public class ActorMapper extends Mapper<LongWritable, Text, NullWritable, Text> {


	private final  IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//InputStream is=new ByteArrayInputStream(currLine.getBytes());
		String currLine = value.toString();
		Reader in = new StringReader(currLine);
		SAXBuilder sb = new SAXBuilder();
        Document doc=null;
		try {
			doc = sb.build(in);
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} //构造文档对象
        Element root = doc.getRootElement(); //获取根元素
        List t = root.getChildren("title");//取名字为disk的所有元素 
        String title=((Element)t.get(0)).getText();
        List list = root.getChildren("actor");
        for (int i = 0; i < list.size(); i++) {
            Element element = (Element) list.get(i);
            String firstName = element.getChild("first_name").getTextTrim();
            String lastName = element.getChild("last_name").getTextTrim();//取disk子元素capacity的内容 
            String birthdate = element.getChild("birth_date").getTextTrim();
            String role = element.getChild("role").getTextTrim();
            word.set(title+"\t"+firstName+" "+lastName+"\t"+birthdate+"\t"+role);
            //context.write(word,null);
            context.write(NullWritable.get(),word);
            
        }
		
		
	//	String filename=((FileSplit) context.getInputSplit()).getPath().getName();
	//	String line = value.toString(); StringTokenizer tokenizer = new StringTokenizer(line); 
		//while (tokenizer.hasMoreTokens()) { word.set(filename+"#"+tokenizer.nextToken()); 
		
	//	context.write(word, one);
		
		
		//output.collect(word, one);
		
	
	}
}
