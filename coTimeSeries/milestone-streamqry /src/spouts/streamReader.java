package spouts;

//import dataOrg;
import main.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class streamReader extends BaseRichSpout {

	private SpoutOutputCollector collector;

	// OutputCollectorBase _collector;

	// private FileReader fileReader;
	private boolean completed = false;

	// .........................................//

	String foldername;
	String filter;

	// static int nstream = 20;
	// public int strePerRow;
	public int curFileCnt = 0;

	public int trowFile;

//	public BufferedReader curbr;
//
//	public File folder;
//	public List<File> list = new ArrayList<File>();
//
//	public FileReader fr;

	public double curStreRand[] = new double[TopologyMain.nstream + 5];
	public double curStreConst[] = new double[TopologyMain.nstream + 5];
	public double curStreBias[] = new double[TopologyMain.nstream + 5];

	// ..........metric related...............//

	public double tupTs = 0.0;
	public double curTs = 0.0;

	public long lcnt = 0;

	public double thputCnt = 0.0;
	public double[] parats = new double[4];
	public int[] parasn = new int[4];
	public int ackcnt = 0;

//	public int tcqSize = 50000;
//	public long[] tcQueue = new long[tcqSize];
//	int tcqSt = 0, tcqEd = 0;
	// ..............................................//

	// FileWriter fstream;
	// BufferedWriter out;

	public int lastStreNum, lastRowNum;

//	public void iniFile(String fileFolder, String fileFilter)
//			throws IOException {
//
//		foldername = fileFolder;
//		filter = fileFilter;
//
//		folder = new File(foldername);
//
//		File folder = new File(foldername);
//		// List<File> list = new ArrayList<File>();
//		getFiles(folder, list, filter);
//
//		Collections.sort(list);
//
//		// fstream = new FileWriter(outfile, true);
//		// out = new BufferedWriter(fstream);
//
//		curFileCnt = 0;
//
//		return;
//	}
//
//	public void getFiles(File folder, List<File> list, String filter) {
//		// folder.setReadOnly();
//		File[] files = folder.listFiles();
//		for (int j = 0; j < files.length; j++) {
//			if (files[j].isDirectory()) {
//				getFiles(files[j], list, filter);
//			} else {
//				// Get only "*-gsensor*" files
//				if (files[j].getName().contains(filter)) {
//					list.add(files[j]);
//				}
//			}
//		}
//	}

//	public int preRowData() throws IOException {
//
//		File curfile;
//		long filelen = 0;
//		int num = list.size();
//
//		if (curFileCnt >= num)
//			return 0;
//
//		curfile = list.get(curFileCnt++);
//
//		// FileReader
//		fr = new FileReader(curfile);
//		curbr = new BufferedReader(fr);
//		String line = "";
//
//		trowFile = 0;
//		line = curbr.readLine();
//
//		return 1;
//	}

//	public int nextRowData(double row[]) throws IOException {
//
//		int cntrow = TopologyMain.nstream / TopologyMain.nstrFile;
//		int tmpcntrow = 0, tmpcntstr = 0, tmp = 0;
//		String line = "", tmpstr = "";
//		StringTokenizer st;
//
//		// append with the previous finished file
//		if (lastStreNum != 0) {
//			tmpcntrow = lastRowNum;
//			tmpcntstr = lastStreNum;
//
//			lastRowNum = 0;
//			lastStreNum = 0;
//		}
//		while (tmpcntrow < cntrow) {
//			line = curbr.readLine();
//			trowFile++;
//
//			if (line == null) {
//
//				lastRowNum = tmpcntrow;
//				lastStreNum = tmpcntstr;
//				return 0;
//			}
//
//			st = new StringTokenizer(line, ",");
//			tmp = 0;
//			while (st.hasMoreTokens()) {
//				tmpstr = st.nextToken();
//				if (tmp >= TopologyMain.offsetRow) {
//					row[tmpcntstr++] = Double.parseDouble(tmpstr);
//				}
//				tmp++;
//			}
//			tmpcntrow++;
//		}
//		return 1;
//	}

	// ...........override
	// functions......................................................//

	public void msgAna(String msg, double parat[], int paras[]) {
		int l = msg.length();
		for (int i = 0; i < l; ++i) {
			if (msg.charAt(i) == ',') {
				paras[0] = Integer.valueOf(msg.substring(0, i));
				parat[0] = Double.valueOf(msg.substring(i + 1, l - 1));
				return;
			}
		}
	}

	@Override
	public void ack(Object msgId) {

		// System.out.printf("----------------------ack\n");

		thputCnt++;
		// System.out.println("OK:" + msgId);

//		msgAna(msgId.toString(), parats, parasn);
//		long tmp = 0;
//		if (Math.abs(parats[0] - curTs) < 1e-3) {
//			ackcnt++;
//
//			if (ackcnt == TopologyMain.nstream) {
//
//				ackcnt = 0;
//				tmp = System.nanoTime();
//				// System.out.printf(
//				// "Latency for tuple %f : %f ms------------------------\n",
//				// curTs, (double)(System.nanoTime() - lcnt)/(1e+6));
//
//				curTs++;
//
//				lcnt = tcQueue[tcqSt];
//				tcqSt = (tcqSt + 1) % tcqSize;
//			}
//		} else {
//
//		}

	}

	@Override
	public void close() {

		System.out.printf("???????????????????? total tuples: %f", thputCnt);

	}

	public void fail(Object msgId) {
		System.out.println("--------------------------FAIL:" + msgId);
	}

	/**
	 * The only thing that the methods will do It is emit each file line
	 */

	// ...........synthetic data production..................//

	public void synDataEmission(int stren) {

		for (int i = 0; i < stren; ++i) {
			curStreRand[i] += Math.random();

//			collector.emit(new Values(i, tupTs, curStreRand[i] - tupTs
//					* curStreBias[i] + curStreConst[i]));

			 collector.emit(new Values(i, tupTs, curStreRand[i] - tupTs
			 * curStreBias[i] + curStreConst[i]), Integer.toString(i)
			 + ',' + Double.toString(tupTs));

			// this.collector.emit(new Values(i, tupTs, curStreRand[i] - tupTs
			// * curStreBias[i] + curStreConst[i]), Integer.toString(i)
			// + ',' + Double.toString(tupTs));
		}
		return;
	}

	public void synDataIni(int stren) {
		for (int i = 0; i < stren; ++i) {
			curStreRand[i] = Math.random();

			curStreConst[i] = Math.random() * 30;
			curStreBias[i] = Math.random() * 10;
		}
		return;
	}

	// ....................................................//

	// .................real dataset...........................//

//	public void realDataEmission() {
//		double[] streRow = new double[TopologyMain.nstream + 10];
//
//		// System.out.printf("----timestamp:  %f\n",tupTs);
//
//		// this.collector.emit(new Values(tupTs, tupTs,
//		// 1),Double.toString(tupTs));
//
//		// if (completed) {
//		// try {
//		// Thread.sleep(1000);
//		// } catch (InterruptedException e) {
//		// // Do nothing
//		// }
//		// return;
//		// }
//		// try {
//
//		// for (int i = 0; i <= 1; ++i) {
//		//
//		// this.collector.emit(new Values(i, tupTs, 2.1),
//		// Integer.toString(i) + ',' + Double.toString(tupTs));
//		// }
//
//		try {
//			if (nextRowData(streRow) == 1) {
//
//				for (int i = 0; i < TopologyMain.nstream; ++i) {
//
//					this.collector.emit(new Values(i, tupTs, streRow[i]),
//							Integer.toString(i) + ',' + Double.toString(tupTs));
//
//					// System.out.printf("----timestamp:  %f\n",streRow[i]);
//				}
//
//				// System.out.printf("----timestamp:  %f\n",tupTs);
//
//			} else {
//				if (preRowData() == 1) {
//					nextRowData(streRow);
//
//					for (int i = 0; i < TopologyMain.nstream; ++i) {
//
//						this.collector.emit(
//								new Values(i, tupTs, streRow[i]),
//								Integer.toString(i) + ','
//										+ Double.toString(tupTs));
//					}
//				} else {
//					// System.out
//					// .printf("!!!!!!!!!!!!!! All files are finished!\n");
//					completed = true;
//				}
//
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

	// ........................................................//
	@Override
	public void nextTuple() {
		/**
		 * The nextuple it is called forever, so if we have been readed the file
		 * we will wait and then return
		 */

		
		Utils.sleep(50);
		// .........data emission................//

		// realDataEmission();

		synDataEmission(TopologyMain.nstream);

//		final double tmpts = tupTs;
//		final int a = 1;
//		final double b = 2.0;
//
//		collector.emit(new Values(a, tmpts, b));

		// this.collector.emit(new Values(1, tupTs, 2323.2), Integer.toString(2)
		// + ',' + Double.toString(tupTs));

		// ......................................//

		// double[] streRow = new double[TopologyMain.nstream + 10];
		//
		// // System.out.printf("----timestamp:  %f\n",tupTs);
		//
		// // this.collector.emit(new Values(tupTs, tupTs,
		// // 1),Double.toString(tupTs));
		//
		// // if (completed) {
		// // try {
		// // Thread.sleep(1000);
		// // } catch (InterruptedException e) {
		// // // Do nothing
		// // }
		// // return;
		// // }
		// // try {
		//
		// // for (int i = 0; i <= 1; ++i) {
		// //
		// // this.collector.emit(new Values(i, tupTs, 2.1),
		// // Integer.toString(i) + ',' + Double.toString(tupTs));
		// // }
		//
		// try {
		// if (nextRowData(streRow) == 1) {
		//
		// for (int i = 0; i < TopologyMain.nstream; ++i) {
		//
		// this.collector.emit(new Values(i, tupTs, streRow[i]),
		// Integer.toString(i) + ',' + Double.toString(tupTs));
		//
		// // System.out.printf("----timestamp:  %f\n",streRow[i]);
		// }
		//
		// // System.out.printf("----timestamp:  %f\n",tupTs);
		//
		// } else {
		// if (preRowData() == 1) {
		// nextRowData(streRow);
		//
		// for (int i = 0; i < TopologyMain.nstream; ++i) {
		//
		// this.collector.emit(
		// new Values(i, tupTs, streRow[i]),
		// Integer.toString(i) + ','
		// + Double.toString(tupTs));
		// }
		// } else {
		// // System.out
		// // .printf("!!!!!!!!!!!!!! All files are finished!\n");
		// completed = true;
		// }
		//
		// }
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		long tmp = System.nanoTime();
//		tcQueue[tcqEd] = tmp;
//		tcqEd = (tcqEd + 1) % tcqSize;

		tupTs++;

		// } catch (Exception e) {
		// throw new RuntimeException("Error reading tuple", e);
		// } finally {System.out.printf("----------------------open\n");
		// // completed = true;
		// }
	}

	/**
	 * We will create the file and get the collector object
	 */

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector coll) {

		// System.out.printf("----------------------open\n");
		// try {
		//
		// iniFile(conf.get("steamsFile").toString(), conf.get("steamsFilter")
		// .toString());
		//
		// preRowData();
		//
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		synDataIni(TopologyMain.nstream);

		collector = coll;
		// this.collector = collector;
	}

	/**
	 * Declare the output field "word"
	 */

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sn", "ts", "value"));
		// declarer.de
	}
}