package Project;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class DeleteServer {
    private static final String ADDRESS = "127.0.0.1";
    private static final int BROKER_PORT = 7000;
    private static final int SERVER_PORT = 5000;

    public static void main(String[] args) {
        System.out.println("Server on port " + SERVER_PORT + " is deleted");
        List<String> content = new ArrayList<>();
        content.add(ADDRESS);
        content.add(String.valueOf(SERVER_PORT));

        Message m = new Message(MessageSender.SERVER, MessageAction.DEL_SERVER, content);
        try {
            Socket socket = new Socket(ADDRESS, BROKER_PORT);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(m);
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
