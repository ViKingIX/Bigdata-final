package edu.ntu.bigdata.entlink;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.apache.mahout.classifier.bayes.XmlInputFormat;

public class parseWIKI
{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
		{
			InputSource is = new InputSource();
			is.setCharacterStream(new StringReader(value.toString()));
			try
			{
				DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
				Document doc = db.parse(is);
				Element docelem = doc.getDocumentElement();
				String title = docelem.getElementsByTagName("title").item(0).getTextContent();
				String id = docelem.getElementsByTagName("id").item(0).getTextContent();
				String text = docelem.getElementsByTagName("text").item(0).getTextContent();
			}
			catch (ParserConfigurationException e) {}
			catch (SAXException e) {}
			return;
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
		{
			return;
		}
	}

	public static void main(String[] args) throws Exception
	{
		JobConf conf = new JobConf(parseWIKI.class);
		conf.setJobName("parseWIKI");

		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		//conf.setOutputValueClass(PageWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return;
	}
}
