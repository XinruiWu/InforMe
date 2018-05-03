package Project;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ClientMultipleRegister {
    private static final String ADDRESS = "127.0.0.1";
    private static final int BROKER_PORT = 7000;
    private static final String CLIENT_PORT = "6000";
    private static final String TYPE = "MUSIC";


    public static void main(String[] args) {
        List<String> content = new ArrayList<>();
        content.add(ADDRESS);
        content.add(CLIENT_PORT);
        content.add(TYPE);
        System.out.println("Client on port " + CLIENT_PORT + " register with topic " + TYPE);
        Message m = new Message(MessageSender.CLIENT, MessageAction.ADD_CLIENT, content);
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
