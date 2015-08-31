/*
 分析每秒钟各个应用层协议的数量以及长度
 */

package com.nmlab.pangu.BasicStatistics.Bolts;

import java.util.Map;
import java.util.TreeMap;

import com.nmlab.pangu.BasicStatistics.Helpers.Application;

import jpcap.packet.IPPacket;
import jpcap.packet.Packet;
import jpcap.packet.TCPPacket;
import jpcap.packet.UDPPacket;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ApplicationBolt implements IRichBolt {
	private OutputCollector outputCollector;
	private Application application;
	private long time=0;
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	public void execute(Tuple tuple)
	{
		IPPacket ippacket=(IPPacket) tuple.getValueByField("ippacket");//from spout
		if(ippacket.sec-time==0){	
			if(ippacket instanceof TCPPacket)
				{
					TCPPacket tcp=(TCPPacket)ippacket;
					application.Setcount(application.TCP_portmap(tcp.src_port, tcp.dst_port), 1);//将端口号映射后的协议写入application中
					application.Setlength(application.TCP_portmap(tcp.src_port, tcp.dst_port), tcp.data.length);
					
				}
			if(ippacket instanceof UDPPacket)
			{
				UDPPacket udp=(UDPPacket)ippacket;
				application.Setcount(application.UDP_portmap(udp.src_port, udp.dst_port), 1);
				application.Setlength(application.UDP_portmap(udp.src_port, udp.dst_port), udp.data.length);
				
			}
		}
		else
		{
			time=ippacket.sec;
			application.lengthsort();
			System.out.println("length="+application.lengthsort);
			application.countsort();
			System.out.println("count="+application.countsort);
			application.Initapplication();//清空一秒内的数据
		}
	}
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector outputCollector)
	{
		this.application=new Application();
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
