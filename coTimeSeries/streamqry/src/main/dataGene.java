package main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class dataGene {

	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);

	void dataGene() {
		try {
			fstream = new FileWriter("/dataset/synData.txt", true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedWriter out = new BufferedWriter(fstream);
		return;
	}

	
	public void generate() throws IOException {
		// try {
		//
		// // fstream = new FileWriter("gridRes.txt", true);
		// // BufferedWriter out = new BufferedWriter(fstream);
		//
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		try {
			fstream = new FileWriter("dataset/synData.txt", false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedWriter out = new BufferedWriter(fstream);
		
		double seed=4.0,val=0.0;
		for (int j = 0; j < 100; j++) {
			
			for (int i = 0; i < TopologyMain.nstream; ++i) {

				if (i < 3) {
					
					val=seed*(i+1)+Math.random()*0.2;
//					System.out.printf(" %f ",val);
					out.write(Double.toString(val)+",");

				}
				else
				{
					val=Math.random()*100;
//					System.out.printf(" %f ",val);
					out.write(Double.toString(val)+",");
				}
			}
			out.write("\n");
			
//			System.out.printf("\n");
		}
		out.close();
//		System.out.printf("!!!  Data set is produced\n");
		return;
	}

}
