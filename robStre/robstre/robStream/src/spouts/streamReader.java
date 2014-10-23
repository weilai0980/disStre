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

	private boolean completed = false;

	String foldername;
	String filter;

	// ........file reader.............//
	public int strePerRow;
	public int curFileCnt = 0;

	public int trowFile;

	public BufferedReader curbr;

	public File folder;
	public List<File> list = new ArrayList<File>();

	public FileReader fr;
	// .........................................//

	public double curStreRand[] = new double[TopologyMain.nstream + 5];
	public double curStreConst[] = new double[TopologyMain.nstream + 5];
	public double curStreBias[] = new double[TopologyMain.nstream + 5];

	// ..........metric related...............//

	public double tupTs = 0.0;
	// for long sliding window
	// public double tupTs =TopologyMain.winSize-1;

	public long lcnt = 0;
	public double thputCnt = 0.0;

	// ...............read from data files...................//
	public int lastStreNum, lastRowNum;

	public void iniFile(String fileFolder, String fileFilter)
			throws IOException {

		foldername = fileFolder;
		filter = fileFilter;
		folder = new File(foldername);

		File folder = new File(foldername);
		getFiles(folder, list, filter);
		Collections.sort(list);

		// fstream = new FileWriter(outfile, true);
		// out = new BufferedWriter(fstream);

		curFileCnt = 0;

		return;
	}

	public void getFiles(File folder, List<File> list, String filter) {
		// folder.setReadOnly();
		File[] files = folder.listFiles();
		for (int j = 0; j < files.length; j++) {
			if (files[j].isDirectory()) {
				getFiles(files[j], list, filter);
			} else {
				// Get only "*-gsensor*" files
				if (files[j].getName().contains(filter)) {
					list.add(files[j]);
				}
			}
		}
	}

	public int preRowData() throws IOException {

		File curfile;
		int num = list.size();

		if (curFileCnt >= num) {

			// .............for loop reading files...........
			// curFileCnt=0;
			System.out.printf("file finished");
			// ..............................................

			return 0;
		}

		curfile = list.get(curFileCnt++);

		// FileReader
		fr = new FileReader(curfile);
		curbr = new BufferedReader(fr);

		trowFile = 0;
		String line = curbr.readLine();

		return 1;
	}

	public int nextRowData(double row[]) throws IOException {

		int cntrow = TopologyMain.nstream / TopologyMain.nstrFile;
		int tmpcntrow = 0, tmpcntstr = 0, tmp = 0;
		String line = "", tmpstr = "";
		StringTokenizer st;

		// append with the previous finished file
		if (lastStreNum != 0) {
			tmpcntrow = lastRowNum;
			tmpcntstr = lastStreNum;

			lastRowNum = 0;
			lastStreNum = 0;
		}
		while (tmpcntrow < cntrow) {
			line = curbr.readLine();
			trowFile++;

			if (line == null) {

				lastRowNum = tmpcntrow;
				lastStreNum = tmpcntstr;
				return 0;
			}

			st = new StringTokenizer(line, ",");
			tmp = 0;
			while (st.hasMoreTokens()) {
				tmpstr = st.nextToken();
				if (tmp >= TopologyMain.offsetRow) {
					row[tmpcntstr++] = Double.parseDouble(tmpstr);
				}
				tmp++;
			}
			tmpcntrow++;
		}
		return 1;
	}

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

	public void realDataEmission() {
		double[] streRow = new double[TopologyMain.nstream + 10];

		try {
			if (nextRowData(streRow) == 1) {

				for (int i = 0; i < TopologyMain.nstream; ++i) {

					this.collector.emit("dataStre", new Values(i, tupTs,
							streRow[i]),
							Integer.toString(i) + ',' + Double.toString(tupTs));
				}
			} else {
				if (preRowData() == 1) {
					nextRowData(streRow);

					for (int i = 0; i < TopologyMain.nstream; ++i) {

						this.collector.emit(
								"dataStre",
								new Values(i, tupTs, streRow[i]),
								Integer.toString(i) + ','
										+ Double.toString(tupTs));
					}
				} else {
					completed = true;
				}

			}

			collector.emit("contrStre", new Values("done"));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// .................................................................//

	/**
	 * The only thing that the methods will do It is emit each file line
	 */

	// ...........synthetic data production..................//

	public void synDataEmission(int stren) {

		int i = 0;
		for (i = 0; i < stren; ++i) {
			curStreRand[i] += Math.random() * 5000;

			collector.emit("dataStre", new Values(i, tupTs, curStreRand[i]
					- tupTs * curStreBias[i] + curStreConst[i]));
		}

		
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		collector.emit("contrStre", new Values("done"+Double.toString(tupTs)));

		return;
	}

	public void synDataIni(int stren) {
		for (int i = 0; i < stren; ++i) {
			curStreRand[i] = Math.random() * 5000;

			curStreConst[i] = Math.random() * 3000; // 2000
			curStreBias[i] = Math.random() * 5000;// 4000

		}
		return;
	}

	// ....................................................//

	@Override
	public void ack(Object msgId) {

		thputCnt++;

	}

	@Override
	public void close() {

		System.out.printf("???????????????????? total tuples: %f", thputCnt);

	}

	public void fail(Object msgId) {
		System.out.println("--------------------------FAIL:" + msgId);
	}

	// ........................................................//
	@Override
	public void nextTuple() {
		/**
		 * The nextuple it is called forever, so if we have been readed the file
		 * we will wait and then return
		 */

		// .........data emission................//

		try {
			Thread.sleep(TopologyMain.tinterval);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (TopologyMain.datasrc == 0) {

			synDataEmission(TopologyMain.nstream);
		} else {
			realDataEmission();
		}
		tupTs++;
	}

	/**
	 * We will create the file and get the collector object
	 */

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector coll) {

		if (TopologyMain.datasrc == 1) {
			// ..........for real data...............//

			try {

				iniFile(conf.get("steamsFile").toString(),
						conf.get("steamsFilter").toString());

				preRowData();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			// ...........for synthetic data..........//

			synDataIni(TopologyMain.nstream);

			// ...................................//
		}

		collector = coll;
	}

	/**
	 * Declare the output field "word"
	 */

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("dataStre", new Fields("sn", "ts", "value"));
		declarer.declareStream("contrStre", new Fields("command"));
	}
}