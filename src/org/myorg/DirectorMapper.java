package org.myorg;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class DirectorMapper extends Mapper<LongWritable, Text, NullWritable, Text> {


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
        List y = root.getChildren("year");//取名字为disk的所有元素 
        String year=((Element)y.get(0)).getText();
        List list = root.getChildren("director");
        for (int i = 0; i < list.size(); i++) {
            Element element = (Element) list.get(i);
            String firstName = element.getChild("first_name").getTextTrim();
            String lastName = element.getChild("last_name").getTextTrim();//取disk子元素capacity的内容 
            word.set(firstName+" "+lastName+"\t"+title+"\t"+year);
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