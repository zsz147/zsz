/*该bolt对于http内容长度的分析，只能分析响应中有content length字段的部分，即静态响应。动态响应中含有*/
package com.nmlab.pangu.BasicStatistics.Bolts;

import java.util.ArrayList;
import java.util.Map;

import com.nmlab.pangu.BasicStatistics.Helpers.Application;
import com.nmlab.pangu.BasicStatistics.Helpers.Http;
import com.nmlab.pangu.BasicStatistics.Helpers.Gethttp;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import jpcap.JpcapCaptor;
import jpcap.NetworkInterface;
import jpcap.NetworkInterfaceAddress;
import jpcap.packet.IPPacket;
import jpcap.packet.Packet;
import jpcap.packet.TCPPacket;

public class GetHttpBolt implements IRichBolt{
	private OutputCollector outputCollector;
	private int httptypecount=0;
	private Application application;
	private long time=0;
	private ArrayList gethttplist;
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	public void execute(Tuple tuple)
	{
		int flag=0;
		Packet packet=(Packet) tuple.getValueByField("tcp");
		if(packet instanceof TCPPacket)
		{
		TCPPacket tcp=(TCPPacket)packet;
		if(tcp.src_port==80||tcp.dst_port==80)
		{
			if(tcp.data.length>0)
			{
				System.out.print("timestamp:"+tcp.usec+ "  ");
	            System.out.print("protocol:" +tcp.protocol + "  ");
	            System.out.print("src IP:" + tcp.src_ip.getHostAddress() + "  ");
	            System.out.print("dst IP:" + tcp.dst_ip.getHostAddress() + "  ");
	            System.out.println("ip.data.length:" + tcp.data.length + "  ");
	            System.out.println("ip.length:" + tcp.length + "  ");
				for(int i=0;i<tcp.data.length;i++)
        		{
        			System.out.print((char)tcp.data[i]);
        		}
			if((char)tcp.data[0]=='G'&&(char)tcp.data[1]=='E'&&(char)tcp.data[2]=='T')//判断是否为get请求
			{
				String tmpstring=null;//用于暂时存储hostname
				for(int i=0;i<gethttplist.size();i++)//对现有请求进行遍历，判断是否为同一会话的多次请求
				{
					if((tcp.src_ip.getHostAddress()).equals(((Gethttp)gethttplist.get(i)).src_ip)&&
					   (tcp.dst_ip.getHostAddress()).equals(((Gethttp)gethttplist.get(i)).dst_ip))
					{
						((Gethttp)gethttplist.get(i)).getcount+=1;
						System.out.println("getcount:"+((Gethttp)gethttplist.get(i)).getcount);
						flag=1;//当前会话存在置1
						break;
					}
				}
				if(flag==0)//会话不存在建立新的会话
				{	
					for(int i=0;i<tcp.data.length;i++)
	        		{
	        			if((char)tcp.data[i]=='H'&&(char)tcp.data[i+1]=='o'&&(char)tcp.data[i+2]=='s'&&(char)tcp.data[i+3]=='t')
	        			{
	        				//System.out.println("hostname");
	        				for(int j=i;j<tcp.data.length;j++)
	        				{
	        					if((int)tcp.data[j]==13&&(int)tcp.data[j+1]==10)//判断回车换行的位置
	        					{
	        						//System.out.println("in get CRLF");
	        						byte[] tmp=new byte[j-(i+6)];
	        						for(int h=0;h<(j-i-6);h++)
	        						{
	        							tmp[h]=tcp.data[i+6+h];  //tmp存的hostname
	        						}
	        						tmpstring=new String(tmp);
	        						System.out.println(tmpstring);
	        						break;
	        					}
	        				}
	        				break;
	        			}
	        		}
					gethttplist.add(new Gethttp(tcp.src_ip.getHostAddress(),tcp.dst_ip.getHostAddress(),1,0,tcp.sec,tcp.usec,tcp.sec,tcp.usec,0,"unkonwn",tmpstring));
					//建立新的会话Gethttp类
				}
				flag=0;
			}
			if((char)tcp.data[0]=='H'&&(char)tcp.data[1]=='T'&&(char)tcp.data[2]=='T'&&(char)tcp.data[3]=='P')
			{
				for(int i=0;i<gethttplist.size();i++)
				{
					if((tcp.src_ip.getHostAddress()).equals(((Gethttp)gethttplist.get(i)).dst_ip)&&
					   (tcp.dst_ip.getHostAddress()).equals(((Gethttp)gethttplist.get(i)).src_ip))
					{
						for(int j=0;j<tcp.data.length;j++)
		        		{
		        			if((char)tcp.data[j]=='C'&&(char)tcp.data[j+1]=='o'&&(char)tcp.data[j+2]=='n'&&
		        			   (char)tcp.data[j+3]=='t'&&(char)tcp.data[j+4]=='e'&&(char)tcp.data[j+5]=='n'&&
		        			   (char)tcp.data[j+6]=='t'&&(char)tcp.data[j+7]=='-'&&(char)tcp.data[j+8]=='L'&&
		        			   (char)tcp.data[j+9]=='e'&&(char)tcp.data[j+10]=='n'&&(char)tcp.data[j+11]=='g'&&
		        			   (char)tcp.data[j+12]=='t'&&(char)tcp.data[j+13]=='h')
		        			{
		        				int tmp=0;
		        				for(int h=j+16;h<tcp.data.length;h++)
		        				{
		        					if((int)tcp.data[h]==13&&(int)tcp.data[h+1]==10)
		        					{
		        						for(int k=j+16;k<h;k++)
		        						{
		        							   tmp=tmp*10;
		        							   tmp=tmp+(int)tcp.data[k]-48;
		        						}
		        						((Gethttp)gethttplist.get(i)).length+=tmp;
		        						System.out.println("contentlength:"+tmp);
		        						break;
		        					}
		        				}
		        			    break;
		        			}
		        		}
						((Gethttp)gethttplist.get(i)).responsecount+=1;
						System.out.println("getcount:"+((Gethttp)gethttplist.get(i)).getcount+"	"+"responsecount:"+((Gethttp)gethttplist.get(i)).responsecount);
						if(((Gethttp)gethttplist.get(i)).responsecount>=((Gethttp)gethttplist.get(i)).getcount)
						{
							((Gethttp)gethttplist.get(i)).endsec=tcp.sec;
							((Gethttp)gethttplist.get(i)).endusec=tcp.usec;
							System.out.println();
							System.out.println("src_ip:"+((Gethttp)gethttplist.get(i)).src_ip);
							System.out.println("dst_ip:"+((Gethttp)gethttplist.get(i)).dst_ip);
							System.out.println("src_hostname:"+((Gethttp)gethttplist.get(i)).src_hostname);
							System.out.println("dst_hostname:"+((Gethttp)gethttplist.get(i)).dst_hostname);
							System.out.println("start_time: "+"sec:"+((Gethttp)gethttplist.get(i)).startsec+"	"+"usec:"+((Gethttp)gethttplist.get(i)).startusec);
							System.out.println("end_time: "+"sec:"+((Gethttp)gethttplist.get(i)).endsec+"	"+"usec:"+((Gethttp)gethttplist.get(i)).endusec);
							System.out.println("http length:"+((Gethttp)gethttplist.get(i)).length);
							System.out.println();
							gethttplist.remove(i);//当前会话结束，清空数组中的当前会话
							break;
						}
					}
				}
			}
			}
		}
		this.outputCollector.ack(tuple);
		}
	}
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector outputCollector) {
		// TODO Auto-generated method stub
		this.outputCollector = outputCollector;
		this.application=new Application();
		this.gethttplist=new ArrayList();        
	}
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		//outputFieldsDeclarer.declare(new Fields("timestamp","throughput","countPacket"));
	}
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
