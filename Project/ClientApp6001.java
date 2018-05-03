package Project;
import java.io.IOException;

public class ClientApp6001 {
	private static final String ADDRESS = "127.0.0.1";
	private static int BROKER_PORT = 7000;
	private static String TYPE = "NEWS";
	private static int CLIENT_PORT = 6001;

	public static void main(String[] args) {
		try {
			Client client = new Client(ADDRESS, CLIENT_PORT, ADDRESS, BROKER_PORT);
			client.register(TYPE);
			System.out.println("Client on port " + CLIENT_PORT + " register with topic " + TYPE);
			client.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
