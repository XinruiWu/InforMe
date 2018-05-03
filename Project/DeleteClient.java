package Project;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class DeleteClient {
    private static final String ADDRESS = "127.0.0.1";
    private static final int SERVER_PORT = 7000;
    private static final String CLIENT_PORT = "6000";
    private static final String TYPE = "WEATHER";
    
    public static void main(String[] args) {
        System.out.println("==================================================================");
        System.out.println("Client on port " + CLIENT_PORT + " unregister with " + TYPE);
        List<String> content = new ArrayList<>();
        content.add(ADDRESS);
        content.add(CLIENT_PORT);
        content.add(TYPE);
        
        Message m = new Message(MessageSender.CLIENT, MessageAction.DEL_CLIENT, content);
        try {
            Socket socket = new Socket(ADDRESS, SERVER_PORT);
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
