package Project;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class Server {
    private static final int CONTENT_EXPIROR_TIME_INTERVAL = 60000; //Expire content every 60 second;
    private static final int CONTENT_EXPIROR_DIFF = 55000; //Expire content that is 55 sec ago

    private ServerSocket serverSocket;
    private int broker_port;
    private int port;
    private HashMap<String, List<String>> followers;
    private String address;
    private String broker_address;
    private Map<String, Vector<Feed>> contentGroups = new ConcurrentHashMap<>();
    private HashMap<String, List<Client>>  clientGroups = new HashMap<>();
    private ContentGroupExpiror contentGroupExpiror;

    public Server(String broker_address, int broker_port, String address, int port) throws IOException {
        this.followers = new HashMap<>();
        this.broker_address = broker_address;
        this.address = address;
        this.port = port;
        this.broker_port = broker_port;
        this.serverSocket = new ServerSocket(port);
        this.contentGroupExpiror = new ContentGroupExpiror();
    }

    public void start() throws IOException {
        while(true) {
            Socket socket = serverSocket.accept();
            ServerThread serverThread = new ServerThread(Server.this, socket);
            serverThread.start();
            new Thread(contentGroupExpiror).start();
        }
    }

    public void registerToBroker() {
        Socket socket;
        try {
            socket = new Socket(broker_address, broker_port);
            List<String> addAndPort = new ArrayList<>();
            addAndPort.add(address);
            addAndPort.add(String.valueOf(port));
            Message m = new Message(MessageSender.SERVER, MessageAction.ADD_SERVER, addAndPort);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(m);
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

            Message m2;
            Message m3;
            m2 = (Message) ois.readObject();
            if(m2.getAction().equals(MessageAction.ADD_FOLLOWER)) {
                setFollowers((HashMap<String, List<String>>) m2.getContent());
                initialFollowers();
            }

            m3 = (Message) ois.readObject();

            HashMap<String, List<List<String>>> clients_information = (HashMap<String, List<List<String>>>) m3.getContent();

            for(Entry<String, List<List<String>>> entry: clients_information.entrySet()) {
                String type = entry.getKey();
                clientGroups.put(type, new ArrayList<>());
                List<List<String>> informations = entry.getValue();
                for(List<String> information: informations) {
                    Client client = new Client(information.get(0), Integer.parseInt(information.get(1)), information.get(2),  Integer.parseInt(information.get(3)));
                    clientGroups.get(type).add(client);
                }
            }
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void initialFollowers() {

        if(followers.size() != 0 && contentGroups.size() != 0) {
            for(Entry<String, List<String>> entry: followers.entrySet()) {
                String type = entry.getKey();
                String follower_address = entry.getValue().get(0);
                int follower_port = Integer.valueOf(entry.getValue().get(1));
                Socket toFollower_Socket;
                try {
                    toFollower_Socket = new Socket(follower_address,follower_port);

                    Vector<Feed> feeds = contentGroups.get(type);
                    Message toFollower_Message = new Message(MessageSender.SERVER, MessageAction.NEW_FEED, feeds);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(toFollower_Socket.getOutputStream());
                    objectOutputStream.writeObject(toFollower_Message);

//                    objectOutputStream.close();
//                    toFollower_Socket.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public HashMap<String, List<String>> getFollowers() {
        return followers;
    }

    public void setFollowers(HashMap<String, List<String>> followers) {
        this.followers = followers;
    }

    public boolean addClients(Client client, String type) {
        System.out.println("==================================================================");
        System.out.println("Server add client at port " + client.getPort() + " for " + type);
//        System.out.println("=========================================");
        for(Entry<String, List<Client>> entry: clientGroups.entrySet()) {
            if(entry.getKey().equals(type)) {
                List<Client> list = entry.getValue();
                list.add(client);
                return true;
            }
        }
        List<Client> list = new ArrayList<>();
        list.add(client);
        clientGroups.put(type, list);
        return true;
    }

    public boolean deleteClient(Client client, String type) {
        System.out.println("==================================================================");
        System.out.println("Server delete client at port " + client.getPort() + " for " + type);
        for(Entry<String, List<Client>> entry: clientGroups.entrySet()) {
            if(entry.getKey().equals(type)) {
                List<Client> list = entry.getValue();
                for(Client c: list){
                    if(c.getAddress().equals(client.getAddress())  && c.getPort() == client.getPort()) {
                        return list.remove(c);
                    }
                }
            }
        }
        return false;
    }

    public List<Client> getClients(String type) {
        for(Entry<String, List<Client>> entry: clientGroups.entrySet()) {
            if(type.equals(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    public void addFeeds(List<Feed> feeds) {
        if(feeds == null || feeds.size() == 0) return;

        String type = feeds.get(0).getType();
        for(Entry<String, Vector<Feed>> entry: contentGroups.entrySet()) {
            if(type.equals(entry.getKey())) {
                Vector<Feed> list = entry.getValue();
                list.addAll(feeds);
                return;
            }
        }
        // 如果找不到的话 new arraylist
        contentGroups.put(type, new Vector<>());
        contentGroups.get(type).addAll(feeds);
    }

    public int getPartitionSize() {
        int size = 0;
        for(Entry<String, Vector<Feed>> entry: contentGroups.entrySet()) {
            size += entry.getValue().size();
        }
        return size;
    }

    private class ContentGroupExpiror implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    expireContent();
                    Thread.sleep(CONTENT_EXPIROR_TIME_INTERVAL);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void expireContent() {
            Timestamp curTimestamp = new Timestamp(System.currentTimeMillis());
            long targetTimestamp = curTimestamp.getTime() - CONTENT_EXPIROR_DIFF;
            Timestamp targetExpireTimestamp = new Timestamp(targetTimestamp);
            for(String key : contentGroups.keySet()) {
                Vector<Feed> feeds = contentGroups.get(key);
                int targetExpireIndex = findExpireIndex(feeds, targetExpireTimestamp);
                if(targetExpireIndex == -1) return;
                for(int i = 0; i <= targetExpireIndex; i++) {
                    feeds.remove(0);
                }
            }
        }

        private int findExpireIndex(Vector<Feed> feedList, Timestamp targetTimestamp) {
            int n = feedList.size();
            int lo = 0;
            int hi = n - 1;
            while(lo < hi) {
                int mid = lo + (hi - lo) / 2;
                if(feedList.get(mid).getTimestamp().equals(targetTimestamp)) {
                    return mid;
                }
                else if(feedList.get(mid).getTimestamp().before(targetTimestamp)) {
                    if(mid < n - 1 && targetTimestamp.before(feedList.get(mid + 1).getTimestamp())){
                        return mid + 1;
                    }
                    lo = mid + 1;
                }
                else {
                    if(mid > 0 && targetTimestamp.after(feedList.get(mid - 1).getTimestamp())){
                        return mid - 1;
                    }
                    hi = mid;
                }
            }
            return -1;
        }
    }

    public void broadCast(List<Feed> feeds, boolean is_follower) {
        System.out.println("==================================================================");
        System.out.println("Server: broadCast to Clients");

        String type = feeds.get(0).getType();
        List<Client> clients = getClients(type);
//        System.out.println("server clients list: " + clients);
        for(Client client: clients) {
            try {
                Socket socket = new Socket(address, client.getPort());
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Message message = new Message(MessageSender.SERVER, MessageAction.NEW_FEED, feeds);
                oos.writeObject(message);

                for(Feed f : feeds) {
                    System.out.println("Feed: " + f.getType() + " " + f.getId());
                }
                Message m2 = (Message) ois.readObject();
                oos.close();
                ois.close();
                socket.close();
            } catch (Exception e) {

                if(!is_follower) {
                    System.out.println("Server: Catch Exception: " + e.getMessage());
                    System.out.println("Client not respond, let follower send feeds");
                    Socket socket = null;
                    try {
                        if(followers.size() == 0) return;
                        for(Entry<String, List<String>> entry: followers.entrySet()) {
                            if(type.equals(entry.getKey())) {
                                List<String> follower_addAndPort = entry.getValue();
                                socket = new Socket(follower_addAndPort.get(0), Integer.parseInt(follower_addAndPort.get(1)));
                            }
                        }
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        Message message = new Message(MessageSender.SERVER, MessageAction.FOLLOWER_SEND, feeds);
                        oos.writeObject(message);

                    } catch (Exception e2) {

                        System.out.println(e2.getMessage() + " Client may be lost");
                    }
                }
            }
        }
    }

    public void sendCopyToFollower(List<Feed> feeds) {
        Socket socket = null;
        try {
            String type = feeds.get(0).getType();
            if(followers.size() == 0) return;
            for(Entry<String, List<String>> entry: followers.entrySet()) {
                if(type.equals(entry.getKey())) {
                    List<String> follower_addAndPort = entry.getValue();
                    socket = new Socket(follower_addAndPort.get(0), Integer.parseInt(follower_addAndPort.get(1)));
                    System.out.println("==================================================================");
                    System.out.println("Server at port " + port + " send copy to follower at port " + Integer.parseInt(follower_addAndPort.get(1)));
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message message = new Message(MessageSender.SERVER, MessageAction.NEW_FEED, feeds);
                    oos.writeObject(message);
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("WARNING " + e.getMessage());
            System.out.println("Server at port " + port + " can't connect to follower");
        }
    }

    public String getBroker_address() {
        return broker_address;
    }

    public int getBroker_port() {
        return broker_port;
    }

    public int getPort() {
        return port;
    }

    public String getAddress() {
        return address;
    }
}

