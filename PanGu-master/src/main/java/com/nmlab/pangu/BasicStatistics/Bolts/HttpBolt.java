/*依据ip不同对http数据流进行定义，统计1秒中共有多少数据流，以及每个流的长度*/

package com.nmlab.pangu.BasicStatistics.Bolts;

import java.util.ArrayList;
import java.util.Map;

import com.nmlab.pangu.BasicStatistics.Helpers.Application;
import com.nmlab.pangu.BasicStatistics.Helpers.Http;


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

public class HttpBolt implements IRichBolt{
	private OutputCollector outputCollector;
	private Http[] http;
	private int httptypecount=0;
	private Application application;
	private long time=0;
	private ArrayList httplist;
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	public void execute(Tuple tuple)
	{
		int flag=0;
		Packet packet=(Packet) tuple.getValueByField("ippacket");
		if(packet instanceof TCPPacket)
		{
		TCPPacket tcp=(TCPPacket)packet;
		if(tcp.src_port==80||tcp.dst_port==80)
		{
			if(tcp.sec-time==0){
			//System.out.println("tcp.sec:"+tcp.sec);
    		application.Setlength("http",tcp.data.length);
    		application.Setcount("http",1);
    		/*System.out.println("This is a http");
    		System.out.println("HTTP.length:"+tcp.data.length);
    		System.out.println("TCP.length:"+tcp.length);
    		System.out.println("Hmlength:"+application.Getlength("http"));
    		System.out.println("Http.sumcount:"+application.Getcount("http"));*/
    		/*for(int i=0;i<tcp.data.length;i++)
    		{
    			System.out.print((char)tcp.data[i]);
    		}*/
    		
    		for(int i=0;i<httplist.size();i++)
    		{
    			if((tcp.src_ip.getHostAddress()).equals(((Http)httplist.get(i)).Getsrc_ip()))
    			{
    				if((tcp.dst_ip.getHostAddress()).equals(((Http)httplist.get(i)).Getdst_ip()))
    				{
    					((Http)httplist.get(i)).Setlength(tcp.data.length);
    					((Http)httplist.get(i)).Setcount(tcp.data.length);
    					flag=1;
    					/*System.out.println();
    					System.out.println("src IP:" + tcp.src_ip.getHostAddress() + "  ");
    					System.out.println("dst IP:" + tcp.dst_ip.getHostAddress() + "  ");
    					System.out.println(i+":length:" + http[i].Getlength() + "  "+"count:"+http[i].Getcount());*/
    					break;
    				}
    				
    			}
    			
    		}
    		if(flag==0)
			{
    			httplist.add(new Http(tcp.data.length,1,tcp.src_ip.getHostAddress(),tcp.dst_ip.getHostAddress()));
			}
			}
			else
			{
				time=tcp.sec;
				System.out.println("httplength:"+application.Getlength("http")+" httpcount:"+application.Getcount("http"));
				for(int i=0;i<httplist.size();i++)
				{
					System.out.print("src IP:" + ((Http)httplist.get(i)).Getsrc_ip() + "  ");
					System.out.print("dst IP:" + ((Http)httplist.get(i)).Getdst_ip() + "  ");
					System.out.print(i+":"+"length:" + ((Http)httplist.get(i)).Getlength() + "  "+"count:"+((Http)httplist.get(i)).Getcount()+"\n");
				}
				application.Initapplication();
				httplist.clear();
	    		application.Setlength("http",tcp.data.length);
	    		application.Setcount("http",1);
				httplist.add(new Http(tcp.data.length,1,tcp.src_ip.getHostAddress(),tcp.dst_ip.getHostAddress()));
			}
    	
		}
		this.outputCollector.ack(tuple);
		}
	}
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector outputCollector) {
		// TODO Auto-generated method stub
		this.outputCollector = outputCollector;
		this.application=new Application();
		this.httplist=new ArrayList();
		http=new Http[1000];
		for(int i=0;i<1000;i++)
		{
			http[i]=new Http();
		}        
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
