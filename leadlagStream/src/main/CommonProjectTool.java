package main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

public class CommonProjectTool {

	FileWriter fstream; // =new FileWriter("", true);
	BufferedWriter out; // = new BufferedWriter(fstream);

	int h =200;
	double[] vec1 = new double[h + 1];
	double[] vec2 = new double[h + 1];
	double vnorm1 = 0, vnorm2 = 0;

	double[] s1 = new double[h + 1];
	double[] s2 = new double[h + 1];

	double[] ns1 = new double[h + 1];
	double[] ns2 = new double[h + 1];

	double[] prj1 = new double[4];
	double[] prj2 = new double[4];

	double[] resi1 = new double[h + 5];
	double[] resi2 = new double[h + 5];

	double rnorm1 = 0, rnorm2 = 0;

	double[] pvec1 = new double[h + 5];
	double[] pvec2 = new double[h + 5];

	double m1 = 0, m2 = 0;
	double v1 = 0, v2 = 0;

	public CommonProjectTool(int size) throws IOException {
		h = size;
		hyperPlaneBuild(h);
		test_generateStream();

		return;
	}

	public void hyperPlaneBuild(int h) throws IOException {
		try {
			fstream = new FileWriter("hyperplane-vectors.txt", false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		double tmp = 0, tmp1=0, tmp2=0;

		for (int i = 0; i < h; ++i) {
			vec1[i] = Math.random();
			vec2[i] = Math.random();

			tmp += (vec1[i] * vec2[i]);
		}

		vec2[h - 1] = -(tmp - (vec1[h - 1] * vec2[h - 1])) / vec1[h - 1];
		
		for(int i=0;i<h;++i)
		{
			tmp1+=(vec1[i]*vec1[i]);
			tmp2+=(vec2[i]*vec2[i]);
		}
		
		for(int i=0;i<h;++i)
		{
			vec1[i]= vec1[i]/ Math.sqrt(tmp1);
			vec2[i]= vec2[i]/ Math.sqrt(tmp2);
		}
		
	    tmp1=0;tmp2=0;
		for(int i=0;i<h;++i)
		{
			tmp1+= vec1[i]* vec1[i];
			tmp2+=vec2[i]* vec2[i];
		}
		System.out.printf("------------- normalized vector : %f %f\n ", tmp1, tmp2);
	
		

		BufferedWriter out = new BufferedWriter(fstream);

		for (int i = 0; i < h; ++i) {
			out.write(Double.toString(vec1[i]) + ",");
		}
		out.write("\n");
		for (int i = 0; i < h; ++i) {
			out.write(Double.toString(vec2[i]) + ",");
		}
		out.close();

		return;
	}

	public void test_generateStream() {
		for (int i = 0; i < h; ++i) {
			s1[i] = Math.random() * 10;
			s2[i] = Math.random() * 30;

		}
		return;

	}

	public void normstream() {
		double tmp1 = 0, tmp2 = 0;
		for (int i = 0; i < h; ++i) {
			tmp1 += s1[i];
			tmp2 += s2[i];
		}
		m1 = tmp1 / h;
		m2 = tmp2 / h;

		tmp1 = tmp2 = 0;

		for (int i = 0; i < h; ++i) {
			tmp1 += ((s1[i] - m1) * (s1[i] - m1));
			tmp2 += ((s2[i] - m2) * (s2[i] - m2));
		}
		v1 = tmp1 / h;
		v2 = tmp2 / h;

		for (int i = 0; i < h; ++i) {
			ns1[i] = (s1[i] - m1) / Math.sqrt(v1 * h);
			ns2[i] = (s2[i] - m2) / Math.sqrt(v2 * h);
		}

		return;
	}

	public double correlationFromDis() {
		double tmp = 0;
		normstream();

		for (int i = 0; i < h; ++i) {
			tmp += ((ns1[i] - ns2[i]) * (ns1[i] - ns2[i]));
		}
		return (2 - tmp) / 2;
	}

	public double correlationDirect() {
		double tmp = 0;
		double tmp1 = 0, tmp2 = 0;
		for (int i = 0; i < h; ++i) {
			tmp1 += s1[i];
			tmp2 += s2[i];
		}
		m1 = tmp1 / h;
		m2 = tmp2 / h;

		tmp1 = tmp2 = 0;

		for (int i = 0; i < h; ++i) {
			tmp1 += ((s1[i] - m1) * (s1[i] - m1));
			tmp2 += ((s2[i] - m2) * (s2[i] - m2));

			tmp += ((s1[i] - m1) * (s2[i] - m2));
		}

		return tmp / Math.sqrt(tmp1 * tmp2);
	}

	
//	---------common project ----------------------------
	
	double[][] cpOperator = new double[3][h + 3];

	public void cp_operator() {
		double norm1 = 0.0, norm2 = 0.0, norm12 = 0.0;
		for (int i = 0; i < h; ++i) {
			norm1 += vec1[i] * vec1[i];
			norm2 += vec2[i] * vec2[i];
			norm12 += vec1[i] * vec2[i];

			

		}
		
		vnorm1 = norm1;
		vnorm2 = norm2;
		
		double tmp = norm1 * norm2;
		double[][] sq = new double[3][3];
		sq[0][0] = norm2 / tmp;
		sq[0][1] = 0;
		sq[1][0] = 0;
		sq[1][1] = norm1 / tmp;

		for (int i = 0; i < h; ++i) {
			cpOperator[0][i] = sq[0][0] * vec1[i] ;
			cpOperator[1][i] = sq[1][1] * vec2[i];

		}

	}

//	public static BigDecimal sqrt(BigDecimal value) {
//	    BigDecimal x = new BigDecimal(Math.sqrt(value.doubleValue()));
//	    return x.add(new BigDecimal(value.subtract(x.multiply(x)).doubleValue() / (x.doubleValue() * 2.0)));
	
//	public static BigDecimal sqrt(BigDecimal A, final int SCALE) {
//	    BigDecimal x0 = new BigDecimal("0");
//	    BigDecimal x1 = new BigDecimal(Math.sqrt(A.doubleValue()));
//	    while (!x0.equals(x1)) {
//	        x0 = x1;
//	        x1 = A.divide(x0, SCALE, ROUND_HALF_UP);
//	        x1 = x1.add(x0);
//	        x1 = x1.divide(TWO, SCALE, ROUND_HALF_UP);
//
//	    }
//	    return x1;
//	}
	
	  final BigDecimal SQRT_DIG = new BigDecimal(100);
	  final BigDecimal SQRT_PRE = new BigDecimal(10).pow(SQRT_DIG.intValue());

	/**
	 * Private utility method used to compute the square root of a BigDecimal.
	 * 
	 * @author Luciano Culacciatti 
	 * @url http://www.codeproject.com/Tips/257031/Implementing-SqrtRoot-in-BigDecimal
	 */
	  BigDecimal sqrtNewtonRaphson  (BigDecimal c, BigDecimal xn, BigDecimal precision){
	    BigDecimal fx = xn.pow(2).add(c.negate());
	    BigDecimal fpx = xn.multiply(new BigDecimal(2));
	    BigDecimal xn1 = fx.divide(fpx,2*SQRT_DIG.intValue(),RoundingMode.HALF_DOWN);
	    xn1 = xn.add(xn1.negate());
	    BigDecimal currentSquare = xn1.pow(2);
	    BigDecimal currentPrecision = currentSquare.subtract(c);
	    currentPrecision = currentPrecision.abs();
	    if (currentPrecision.compareTo(precision) <= -1){
	        return xn1;
	    }
	    return sqrtNewtonRaphson(c, xn1, precision);
	}

	/**
	 * Uses Newton Raphson to compute the square root of a BigDecimal.
	 * 
	 * @author Luciano Culacciatti 
	 * @url http://www.codeproject.com/Tips/257031/Implementing-SqrtRoot-in-BigDecimal
	 */
	public BigDecimal bigSqrt(BigDecimal c){
	    return sqrtNewtonRaphson(c,new BigDecimal(1),new BigDecimal(1).divide(SQRT_PRE));
	}
	
	public static BigDecimal sqrt(BigDecimal x) {
		return BigDecimal.valueOf(StrictMath.sqrt(x.doubleValue()));
	}
	
	public void cp_project() {
		
		prj1[0] =0;
		prj1[1] =0;

		prj2[0]=0;
		prj2[1]=0;
		
		for (int i = 0; i < h; ++i) {
			prj1[0] += (cpOperator[0][i] * ns1[i]);
			prj1[1] += (cpOperator[1][i] * ns1[i]);

			prj2[0] += (cpOperator[0][i] * ns2[i]);
			prj2[1] += (cpOperator[1][i] * ns2[i]);
		}

		BigDecimal rn1 = new BigDecimal(0), rn2 = new BigDecimal(0);
		BigDecimal tmp = new BigDecimal(0);
		
		for (int i = 0; i < h; ++i) {
			pvec1[i] = prj1[0] * vec1[i] + prj1[1] * vec2[i];
			pvec2[i] = prj2[0] * vec1[i] + prj2[1] * vec2[i];

			resi1[i] = prj1[0] * vec1[i] + prj1[1] * vec2[i] - ns1[i];
			resi2[i] = prj2[0] * vec1[i] + prj2[1] * vec2[i] - ns2[i];
			
			tmp = new BigDecimal((prj1[0] * vec1[i] + prj1[1] * vec2[i] - ns1[i]) * (prj1[0] * vec1[i] + prj1[1] * vec2[i] - ns1[i]));
			rn1=rn1.add(tmp);
			
			tmp = new BigDecimal((prj2[0] * vec1[i] + prj2[1] * vec2[i] - ns2[i]) * (prj2[0] * vec1[i] + prj2[1] * vec2[i] - ns2[i]));
			rn2=rn2.add(tmp);
			
		}

		
		rnorm1=0;
		rnorm2=0;
		for (int i = 0; i < h; ++i) {
			rnorm1 += resi1[i] * resi1[i];
			rnorm2 += resi2[i] * resi2[i];
		}

		
		double prt1 = 0, prt2 = 0;
		for (int i = 0; i < h; ++i) {
			prt1 += pvec1[i] * resi1[i];
			prt2 += pvec2[i] * resi2[i];
		}

		System.out
				.printf(" ++++++++++++++++++++++++++ product between residual and projected vector: %f %f\n",
						prt1, prt2);

		prt1 = 0;
		prt2 = 0;
		for (int i = 0; i < h; ++i) {
			prt1 += vec1[i] * resi1[i];
			prt2 += vec1[i] * resi2[i];
		}
		System.out
				.printf(" ++++++++++++++++++++++++++  product between residual and vector1:  %f   %f\n",
						prt1, prt2);

		prt1 = 0;
		prt2 = 0;
		for (int i = 0; i < h; ++i) {
			prt1 += vec2[i] * resi1[i];
			prt2 += vec2[i] * resi2[i];
		}
		System.out
				.printf(" ++++++++++++++++++++++  product between residual and vector2: %f  %f\n",
						prt1, prt2);
		
		
		prt1 = 0;
		prt2 = 0;
		for (int i = 0; i < h; ++i) {
			prt1 += vec1[i] * vec2[i];
		}
		System.out
				.printf(" ++++++++++++++++++++++  product between vec1 and vector2:   %f\n",
						prt1);
		
		

		double rp1 = 0;
		for (int i = 0; i < h; ++i) {
			rp1 += resi1[i] * resi2[i];
		}

		BigDecimal normprod = new BigDecimal(Math.sqrt(rnorm1)
				* Math.sqrt(rnorm2));


		BigDecimal normprod1 = bigSqrt(rn1).multiply( bigSqrt(rn2)); 
	
	
		System.out.printf(
				" ++++++++++++++++++++++  residual product: %f  %f  %f \n", rp1,
				rn1,rn2);
		
		
		System.out.printf(
				" ????????  cos(angle) : %f \n", rp1/ Math.sqrt(rnorm1)/Math.sqrt(rnorm2) );
		
		// ,rp1/normprod. );

		return;
	}

	public double cp_correlation(double corre[]) {
		cp_operator();
		cp_project();

		double rp1 = 0;
		for (int i = 0; i < h; ++i) {
			rp1 += resi1[i] * resi2[i];
		}
		// Math.sqrt(rnorm1)*Math.sqrt(rnorm2)
		double tmp = Math.sqrt(rnorm1)*Math.sqrt(rnorm2);
			

		double cp_dis1 = 0, cp_dis2 = 0;
		cp_dis1 = (prj1[0] * prj1[0] - prj1[0] * prj2[0]) * vnorm1
				+ (prj1[1] * prj1[1] - prj1[1] * prj2[1]) * vnorm2 + rnorm1
				- tmp + (-prj2[0] * prj1[0] + prj2[0] * prj2[0]) * vnorm1
				+ (-prj2[1] * prj1[1] + prj2[1] * prj2[1]) * vnorm2 - tmp
				+ rnorm2;

		cp_dis2 = (prj1[0] * prj1[0] - prj1[0] * prj2[0]) * vnorm1
				+ (prj1[1] * prj1[1] - prj1[1] * prj2[1]) * vnorm2 + rnorm1
				+ tmp + (-prj2[0] * prj1[0] + prj2[0] * prj2[0]) * vnorm1
				+ (-prj2[1] * prj1[1] + prj2[1] * prj2[1]) * vnorm2 + tmp
				+ rnorm2;

		corre[0] = (2 - cp_dis1) / 2;
		corre[1] = (2 - cp_dis2) / 2;

		
		
//		................another way............
		
		double dif= Math.abs( Math.sqrt(rnorm1)-Math.sqrt(rnorm2))*Math.abs( Math.sqrt(rnorm1)-Math.sqrt(rnorm2));
		double dis= (prj1[0]-prj2[0])*(prj1[0]-prj2[0]) + (prj1[1]-prj2[1])*(prj1[1]-prj2[1]);
		
		System.out.printf(
				" ????????+++++++   %f \n", (2-(dif+dis))/2  );
		
		
		
		return 0;
	}

}