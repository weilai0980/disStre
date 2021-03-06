import java.io.IOException;


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


public class coorGenerationTest {


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

	public double curStreRand[] = new double[20+ 5];
	public double curStreConst[] = new double[20+ 5];
	public double curStreBias[] = new double[20+ 5];

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
			// System.out.printf("file finished ");
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

		int cntrow = 20 / 20;
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
				if (tmp >= 0) {
					row[tmpcntstr++] = Double.parseDouble(tmpstr);
				}
				tmp++;
			}
			tmpcntrow++;
		}
		return 1;
	}



	public void realDataEmission() {
		double[] streRow = new double[20 + 10];

		try {
			// .....test...........
//			nextRowData(streRow);
			// ....................

			if (nextRowData(streRow) == 1) {

				for (int i = 0; i < 20; ++i) {

	
				}
			} else {
				if (preRowData() == 1) {
					nextRowData(streRow);

					for (int i = 0; i < 20; ++i) {

		
					}
				} else {
					completed = true;
				}

			}


		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}






	
	int[] direcVec = {-1,0,1};
	int[] a = {1,2,3};
	
	void calCellCoor( int dftNum,int curCnt, String cellCoor, int coor[]) {

		if(curCnt>=dftNum)
		{
//			System.out.printf("%s\n", cellCoor);
			
			for(int i=0;i<dftNum;++i)
			{
				System.out.printf("%d", coor[i]);
			}
			System.out.printf("\n");
			
			return;
			
		}
		for(int i=0;i<3;++i)
		{
			cellCoor=cellCoor+ Integer.toString(a[curCnt]+direcVec[i])+",";
			
			coor[curCnt]=a[curCnt]+direcVec[i];
			
			calCellCoor(dftNum, curCnt+1, cellCoor,coor);
		
		}
	
		return;
	}

	public static void main(String[] args)  {
		
		String cell=new String();
		int[] res= new int[10];
		coorGenerationTest obj=new coorGenerationTest();
		obj.calCellCoor( 3,0, cell,res);
	
	}
	

}
