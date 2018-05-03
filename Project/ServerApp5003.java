package Project;
import java.io.IOException;

public class ServerApp5003 {
    private static final String ADDRESS = "127.0.0.1";
    private static final int BROKER_PORT = 7000;
    private static final int SERVER_PORT = 5003;

    public static void main(String[] args) {
        System.out.println("Server on port " + SERVER_PORT);
        try {
            Server server = new Server(ADDRESS, BROKER_PORT, ADDRESS, SERVER_PORT);
            server.registerToBroker();
            server.start();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
