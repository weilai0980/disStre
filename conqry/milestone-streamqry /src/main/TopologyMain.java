package main;

import spouts.*;

import bolts.*;
import grouping.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.io.*;

public class TopologyMain {

	// real data

	// public static int nstreBolt = 30;
	// public static double thre = 0.98;
	// public static int nstream = 20;
	// public static int winSize = 3;
	// public static int gridIdxN=1000;
	// public static int nstrFile = 5;
	// public static int offsetRow = 2;

	// synthetic data

	
	//............parallelism record..............//
	
	public static int nstreBolt = 3000;
	public static double thre = 0.98;
	public static int nstream = 3000;
	public static int winSize = 3;
	public static int gridIdxN = 500;
	
	
	//............................................//
	
	public static int nstrFile = 20;
	public static int tasknum = 2;
	public static int offsetRow = 0;

	public static int approBoltNum = 8;
	// public static int streNum =100000;

	// 8;

	public static int wokernum = 60;
	
	public static int preBoltNum = 240;
	public static int calBoltNum = 120;
	public static int aggreBoltNum = 60;

	public static void main(String[] args) throws InterruptedException,
			IOException, AlreadyAliveException, InvalidTopologyException {

		System.out.printf("i am ok\n");

		int appro = Integer.valueOf(args[0]);

		String runenv = args[1];

		// //............group test...................//
		// TopologyBuilder builderNavie = new TopologyBuilder();
		// builderNavie.setSpout("sreader", new streamReader());
		//
		// builderNavie.setBolt("test", new testGroupBolt(),
		// 4).customGrouping("sreader", new testgroup());
		// builderNavie.setBolt("test1", new naivePreBolt(),
		// 4).customGrouping("sreader", new testgroup());

		// .shuffleGrouping("sreader");

		// Configuration
		Config conf = new Config();
		conf.put("steamsFile", "./dataset/"); // needs modify

		conf.put("steamsFilter", "syn"); // needs modify
		// conf.put("steamsFilter", "A"); // needs modify
		conf.setDebug(false);
		
		conf.setNumWorkers(wokernum);

		// Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();

		if (appro == 0) {

			TopologyBuilder builderNavie = new TopologyBuilder();
			builderNavie.setSpout("sreader", new streamReader());

			builderNavie.setBolt("naviepre", new naivePreBolt(), preBoltNum)
					.fieldsGrouping("sreader", new Fields("sn"));
//			
			builderNavie.setBolt("naviestatis", new naiveStatisBolt(), calBoltNum)
					.fieldsGrouping("naviepre", new Fields("tspair"));
			builderNavie.setBolt("navieaggre", new naiveAggreBolt(),aggreBoltNum)
					.fieldsGrouping("naviestatis", new Fields("taskid"));
			
			
			// taskid
			// .fieldsGrouping("naviestatis", new Fields("pair"));

			if (runenv.compareTo("local") == 0) {
				cluster.submitTopology("strqry", conf,
						builderNavie.createTopology());
				Thread.sleep(2000);
			} else if (runenv.compareTo("cluster") == 0) {
				StormSubmitter.submitTopology("conqry", conf,
						builderNavie.createTopology());
			}

		} else if (appro == 1) { // may add hashset or hashmap to boost the
									// performance

			TopologyBuilder builderGrid = new TopologyBuilder();
			builderGrid.setSpout("sreader", new streamReader());

			builderGrid.setBolt("gridpre", new gridPreBolt(), 2)
					.fieldsGrouping("sreader", new Fields("sn"));

			builderGrid.setBolt("gridstatis", new gridStatisBolt(), 8)
					.fieldsGrouping("gridpre", new Fields("coord"));

			builderGrid.setBolt("gridfilter", new gridFilterBolt(), 1)
					.fieldsGrouping("gridstatis", new Fields("pair"));

			if (runenv.compareTo("local") == 0) {
				cluster.submitTopology("strqry", conf,
						builderGrid.createTopology());
				Thread.sleep(2000);
			} else if (runenv.compareTo("cluster") == 0) {
				StormSubmitter.submitTopology("conqry", conf,
						builderGrid.createTopology());
			}

		}

		else if (appro == 2) { // may add hashset or hashmap to boost the
								// performance

			TopologyBuilder builderAdjust = new TopologyBuilder();
			builderAdjust.setSpout("sreader", new streamReader());

			builderAdjust.setBolt("adjPre", new AdjustPreBolt(), 2)
					.fieldsGrouping("sreader", new Fields("sn"));

			builderAdjust.setBolt("adjAppro", new AdjustApproBolt(),
					approBoltNum).customGrouping("adjPre", "interStre",
					new AdjustHashGroup());

			builderAdjust
					.setBolt("adjAggre", new AdjustAggreBolt(), 1)
					.fieldsGrouping("adjPre", "qualStre", new Fields("pair"))
					.fieldsGrouping("adjAppro", "interQualStre",
							new Fields("pair"));

			if (runenv.compareTo("local") == 0) {
				cluster.submitTopology("strqry", conf,
						builderAdjust.createTopology());
				Thread.sleep(2000);
			} else if (runenv.compareTo("cluster") == 0) {
				StormSubmitter.submitTopology("conqry", conf,
						builderAdjust.createTopology());
			}

			// cluster.shutdown();
		}

		// else if(appro==2)
		// {
		// TopologyBuilder builderGrid = new TopologyBuilder();
		// builderGrid.setSpout("sreader", new streamReader());
		//
		// builderGrid.setBolt("precComp", new precComp(), 2)
		// .fieldsGrouping("sreader", new Fields("sn"));
		//
		// // builderGrid.setBolt("gridstatis", new gridStatisBolt(), 1)
		// // .fieldsGrouping("gridpre", new Fields("coord"));
		//
		//
		//
		// cluster.submitTopology("strqry", conf, builderGrid.createTopology());
		// Thread.sleep(2000);
		// }
		else if (appro == 10) {

			dataGene dataProd = new dataGene();
			dataProd.generate();
			System.out.printf("!!   Data set is produced\n");
			// System.out.printf("%f   %f\n", Math.ceil(-1.3),Math.ceil(1.3));
		}

		System.out.printf("topology is finishing");
		cluster.shutdown();

	}
}