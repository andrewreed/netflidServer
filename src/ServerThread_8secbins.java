import java.util.*;
import java.io.*;
import java.net.*;
import java.math.*;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.StatUtils;
import net.sf.javaml.core.kdtree.KDTree;

public class ServerThread implements Runnable {

	private Socket clientSocket;
	private KDTree windowDB;

	public static final int WINDOW_SIZE = 30;

	public ServerThread(Socket clientSocket, KDTree windowDB) {
		this.clientSocket = clientSocket;
		this.windowDB = windowDB;
	}

	@Override
	public void run() {
		PearsonsCorrelation correlator = new PearsonsCorrelation();

		String ipAddr = clientSocket.getInetAddress().toString();

		System.out.println(ipAddr + "\tconnected");

		PrintWriter out = null;
		Scanner in = null;

		try {
			out = new PrintWriter(clientSocket.getOutputStream(), true); 
			in = new Scanner(clientSocket.getInputStream());
		} catch (IOException e) {
			System.err.println(ipAddr + "\treader/writer failed"); 
			return; 
		} 

		while (in.hasNextLine()) {
			String inputLine = in.nextLine();
			String[] tokens = inputLine.split("\\t");

			if (tokens[0].equals("complete")) {
				out.print("\f");
				System.out.println(ipAddr + "\tcomplete");
				break;
			}

			String macAddr = tokens[0];
			String startTime = tokens[1];

			String[] readings = tokens[2].split(",");
			int[] sums = new int[WINDOW_SIZE];

			for (int i = 0; i < WINDOW_SIZE; i++) {
				sums[i] = Integer.parseInt(readings[i]);
			}

			short dummyBitrate = 0;
			Window currentWindow = new Window(startTime, dummyBitrate, 0, sums);
			
			double[] key = currentWindow.getKey();

			double[] lowerKey = new double[]{key[0] * 0.97,
			                                 key[1] - 0.015,
			                                 key[2] - 0.015,
			                                 key[3] - 0.015,
			                                 key[4] - 0.015,
			                                 key[5] - 0.015};

			double[] upperKey = new double[]{key[0] * 1.03,
			                                 key[1] + 0.015,
			                                 key[2] + 0.015,
			                                 key[3] + 0.015,
			                                 key[4] + 0.015,
			                                 key[5] + 0.015};

			Object[] shortList = windowDB.range(lowerKey, upperKey);

			int[] currentSegments = currentWindow.getSegments();

			for (int i = 0; i < shortList.length; i++) {
				Window compareWindow = (Window)shortList[i];
				int compareStart = compareWindow.getStartIndex();
				int[] compareSegments = compareWindow.getSegments();

				double[] current15 = new double[15];
				double[] compare15 = new double[15];

				for (int y = 0; y < 15; y++) {
					current15[y] = (double)currentSegments[2*y] + (double)currentSegments[(2*y) + 1];
					compare15[y] = (double)compareSegments[compareStart + (2*y)] + (double)compareSegments[compareStart + (2*y) + 1];
				}

				double[] currentSnapSamples = new double[9];
				for (int y = 0; y < 9; y++) {
					currentSnapSamples[y] = ((3.0*current15[y]) + 
								                  (-7.0*current15[y+1]) + 
								                   (1.0*current15[y+2]) + 
								                   (6.0*current15[y+3]) + 
								                   (1.0*current15[y+4]) + 
								                  (-7.0*current15[y+5]) + 
								                   (3.0*current15[y+6])) / 11.0;
				}

				double[] compareSnapSamples = new double[9];
				for (int y = 0; y < 9; y++) {
					compareSnapSamples[y] = ((3.0*compare15[y]) + 
								                  (-7.0*compare15[y+1]) + 
								                   (1.0*compare15[y+2]) + 
								                   (6.0*compare15[y+3]) + 
								                   (1.0*compare15[y+4]) + 
								                  (-7.0*compare15[y+5]) + 
								                   (3.0*compare15[y+6])) / 11.0;
				}

				double snapCorrel = correlator.correlation(currentSnapSamples, compareSnapSamples);

				if (snapCorrel < 0.95) {
					continue;
				}

				double posCorrel = correlator.correlation(current15, compare15);

				if (posCorrel < 0.95) {
					continue;
				}

				out.println(macAddr + "\t" +
				            currentWindow.getTitle() + "\t" + 
				            compareWindow.getTitle() + "\t" + 
				            compareWindow.getStartIndex() + "\t" +
				            compareWindow.getKey()[0] / key[0] + "\t" +
				            snapCorrel + "\t" +
				            posCorrel);
			}
		}
		try {
			out.close();
			in.close();
			clientSocket.close();
		} catch (IOException e) { 
			System.err.println(ipAddr + "\tclean-up failed"); 
		} 
		System.out.println(ipAddr + "\tdisconnected");
	}
}
