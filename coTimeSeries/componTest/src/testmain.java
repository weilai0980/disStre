
import java.io.IOException;
import java.util.ArrayList;

//import backtype.storm.generated.AlreadyAliveException;
//import backtype.storm.generated.InvalidTopologyException;
//import main.TopologyMain;

public class testmain {

	int N = 6;
	public ArrayList<Integer> emitStack = new ArrayList<Integer>();
	ArrayList<String> adjCell = new ArrayList<String>();
	public int[] direcVec = { -1, 0, 1 };

	public int winSize = 3;
	public int[] dimSign = { 0, -1, 2 };
	public int[] dimSignBound = { 0, 1, 2 };

	public int emittask = 0;

	public double diviNum = 3.0;

	public void broadcastEmitNoRecur(int orgiCoord[]) {

		int curlay = 0;
		int[] tmpCoor = new int[winSize + 5];

		int stkSize = 0, curdir = -1, tmpdir = 0;
		double tmptaskId = 0.0;

		// .....ini....................//
		if (dimSignBound[curlay] == 0) {
			tmpCoor[curlay] = orgiCoord[curlay];

			emitStack.add(0);
			stkSize++;
		} else if (dimSignBound[curlay] == 1) {

			tmpdir = curdir + 1;
			tmpCoor[curlay] = orgiCoord[curlay] + tmpdir * dimSign[curlay];
			// tmpCoor[curlay] = orgiCoord[curlay] + dimSign[curlay];

			emitStack.add(0);
			stkSize++;
		} else if (dimSignBound[curlay] == 2) {

			tmpdir = curdir + 1;
			tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];

			emitStack.add(0);
			stkSize++;
		}

		curlay++;
		curdir = -1;
		// ...........................//

		int popflag = 0;

		while (emitStack.size() != 0) {

			if (popflag == 1) {
				curdir = emitStack.get(stkSize - 1);
				emitStack.remove(stkSize - 1);
				stkSize--;
				curlay--;

				popflag = 0;
			}

			if (curlay >= winSize) {

				tmptaskId = tmpCoor[0];

				System.out.printf("%d  ", tmpCoor[0]);

				for (int j = 1; j < winSize; ++j) {
					tmptaskId = tmptaskId + (tmpCoor[j] - 1)
							* Math.pow(diviNum, j);

					System.out.printf("%d  ", tmpCoor[j]);

				}

				// if (Math.ceil(tmptaskId) > taskCnt) {
				// tmptaskId = taskCnt;
				// }

				// emittask = (int) Math.ceil(tmptaskId) - 1;
				//
				// if (emittask < 0)
				// emittask = 0;

				System.out.printf("%d\n", (int) Math.ceil(tmptaskId));

				// taskSet.add(_tasks.get(emittask));
				// tasklist.add(_tasks.get(emittask));

				// curdir = emitStack.get(stkSize - 1);
				// emitStack.remove(stkSize - 1);
				// stkSize--;
				// curlay--;

				popflag = 1;

			} else {

				if (curdir + 1 > dimSignBound[curlay]) {

					// curdir = emitStack.get(stkSize - 1);
					// emitStack.remove(stkSize - 1);
					// stkSize--;
					// curlay--;

					popflag = 1;

					continue;
				} else {

					if (dimSignBound[curlay] == 0) {
						tmpCoor[curlay] = orgiCoord[curlay];

						emitStack.add(0);
						stkSize++;

					} else if (dimSignBound[curlay] == 1) {

						tmpdir = curdir + 1;
						tmpCoor[curlay] = orgiCoord[curlay] + tmpdir
								* dimSign[curlay];

						emitStack.add(tmpdir);
						stkSize++;

					} else if (dimSignBound[curlay] == 2) {

						tmpdir = curdir + 1;
						tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];

						emitStack.add(tmpdir);
						stkSize++;
					}

					curlay++;
					curdir = -1;
				}
			}
		}

		return;
	}

	public void gridEmitNoRecur(int orgiCoord[]) {

		int curlay = 0;
		int[] tmpCoor = new int[N + 5];
		String coordstr = new String();

		int stkSize = 0, curdir = 0, i = 0, tmpdir = 0;

		// .....ini....................//
		emitStack.add(curdir);
		stkSize++;
		tmpdir = curdir;

		tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];
		if (tmpCoor[curlay] == 0) {
			if (tmpdir == 0) {
				tmpCoor[curlay] = -1;
			} else if (tmpdir == 2) {
				tmpCoor[curlay] = 1;
			}
		}

		curlay++;
		curdir = -1;
		// ...........................//

		int popflag = 0;
		while (emitStack.size() != 0) {

			// if(curlay==0)
			// {
			// System.out.printf("++++ %d \n",emitStack.get(stkSize - 1));
			// }

			if (popflag == 1) {
				curdir = emitStack.get(stkSize - 1);
				emitStack.remove(stkSize - 1);
				stkSize--;
				curlay--;

				popflag = 0;
			}

			if (curlay >= N) {

				coordstr = "";
				for (i = 0; i < N; ++i) {
					coordstr = coordstr + Integer.toString(tmpCoor[i]) + ",";

					// if (tmpCoor[i] > boundCell)
					// break;
					// if (tmpCoor[i] < -1 * boundCell)
					// break;

				}

				System.out.printf("%s\n", coordstr);

				if (i >= N) {
					adjCell.add(coordstr);
				}

				popflag = 1;

				// curdir = emitStack.get(stkSize - 1);
				// emitStack.remove(stkSize - 1);
				// stkSize--;
				// curlay--;

			} else {

				if (curdir + 1 > 2) {

					popflag = 1;

					// curdir = emitStack.get(stkSize - 1);
					// emitStack.remove(stkSize - 1);
					// stkSize--;
					// curlay--;

					continue;
				} else {

					emitStack.add(curdir + 1);
					stkSize++;

					tmpdir = curdir + 1;
					tmpCoor[curlay] = orgiCoord[curlay] + direcVec[tmpdir];
					if (tmpCoor[curlay] == 0) {
						if (tmpdir == 0) {
							tmpCoor[curlay] = -1;
						} else if (tmpdir == 2) {
							tmpCoor[curlay] = 1;
						}
					}

					curlay++;
					curdir = -1;

				}
			}
		}

		return;
	}

	public static void main(String[] args) {

		int arr[] = { 1, 2, 2 };
		// testmain tester=new testmain();

		// tester.gridEmitNoRecur(arr);

		// tester.broadcastEmitNoRecur(arr);

		int a = 1, b = 4;
		double tmp = 0;

		long tStart = System.currentTimeMillis();

		for (int i = 0; i < 7000; ++i) {
			for (int j = 0; j < 700; ++j) {
				for (int k = 0; k < 600; ++k) {
					tmp = ((a + Math.random()) + (b + Math.random()))
							* ((a + Math.random()) + (b + Math.random()));
				}
			}
		}

		long tEnd = System.currentTimeMillis();
		long tDelta = tEnd - tStart;
		double elapsedSeconds = tDelta / 1000.0;
        System.out.printf("time: %f \n", elapsedSeconds);
		
		return;
	}

}
