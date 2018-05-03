package Project;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Broker {

    private static final String BROKER_ADDRESS = "127.0.0.1";
    private String producerAddress;
    private int producerPortNumber;
    private boolean mainBroker;
    private int brokerPortNumber;
    private ServerSocket brokerSocket;
    private ProducerCaller producerCaller;
    private ServersChecker serversChecker;
    private BackupHandler backupHandler;
    private volatile Map<String, Set<String>> lastFeedVersion;
    private volatile Map<String, List<Client>> subscribeList;
    private volatile List<Server> serverList;
    private volatile List<Server> aliveServerList;
    private volatile Map<Server, Integer> serverLoad;
    private volatile List<BackupBroker> backupBrokerList;
    private volatile List<Producer> producerList;

    private class Producer {
        private String type;
        private String address;
        private int portNumber;
        private boolean isOn;

        private Producer(String type, String address, int portNumber, boolean isOn) {
            this.type = type;
            this.address = address;
            this.portNumber = portNumber;
            this.isOn = isOn;
        }

        public String getType() {
            return type;
        }

        private String getAddress(){
            return this.address;
        }

        private int getPortNumber(){
            return this.portNumber;
        }
    }

    private class BackupBroker {
        private String address;
        private int portNumber;
        private boolean isOn;

        private BackupBroker(String address, int portNumber, boolean isOn) {
            this.address = address;
            this.portNumber = portNumber;
            this.isOn = isOn;
        }

        private String getAddress(){
            return this.address;
        }

        private int getPortNumber(){
            return this.portNumber;
        }

        public void setOn(boolean on) {
            isOn = on;
        }
    }

    private class Server {
        private String address;
        private int portNumber;
        private boolean isOn;
        private boolean needToCheck;

        private Server(String address, int portNumber, boolean isOn, boolean needToCheck) {
            this.address = address;
            this.portNumber = portNumber;
            this.isOn = isOn;
            this.needToCheck = needToCheck;
        }

        public void setNeedToCheck(boolean needToCheck) {
            this.needToCheck = needToCheck;
        }

        private String getAddress(){
            return this.address;
        }

        private int getPortNumber(){
            return this.portNumber;
        }

        public void setOn(boolean on) {
            isOn = on;
        }
    }

    private class Client {
        private String address;
        private int portNumber;
        private boolean isOn;

        private Client(String address, int portNumber, boolean isOn) {
            this.address = address;
            this.portNumber = portNumber;
            this.isOn = isOn;
        }

        public boolean isOn() {
            return isOn;
        }

        private String getAddress(){
            return this.address;
        }

        private int getPortNumber(){
            return this.portNumber;
        }

        public void setOn(boolean on) {
            isOn = on;
        }
    }

    private class ProducerCaller implements Runnable {

        private boolean newType;
        private String type;

        private ProducerCaller(boolean newType, String type) {
            this.newType = newType;
            this.type = type;
        }

        public void run() {
            if (newType){
                System.out.println("Add New Type: " + type);
                addNewType();
            } else {
                checkUpdate();
            }
        }

        private void addNewType() {
            try {
                Socket socket = new Socket(producerAddress, producerPortNumber);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.NEW_TOPIC, this.type));
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                Message newTopic = (Message) objectInputStream.readObject();
                List<String> parameters = (List<String>) newTopic.getContent();
                Producer newProducer = new Producer(type, parameters.get(0), Integer.parseInt(parameters.get(1)), true);
                System.out.println("Add New Producer: " + this.type);
                producerList.add(newProducer);
                lastFeedVersion.put(type, new HashSet<>());
                parameters.add(type);
                copyToBackupBroker(new Message(MessageSender.BROKER, MessageAction.NEW_TOPIC, parameters));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void checkUpdate() {
            while (true){
                for (Producer producer : producerList){
                    if (producer.isOn && !serverList.isEmpty()){
                        try{
                            FeedRetrieval(producer);
                            Thread.sleep(5000);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        public void FeedRetrieval(Producer producer) throws Exception{
            Socket socket = new Socket(producer.getAddress(), producer.getPortNumber());
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            List<Feed> feedList = (List<Feed>) objectInputStream.readObject();



            List<Feed> newFeed = new ArrayList<>();
            Set<String> lastVersion = lastFeedVersion.get(producer.getType());
            for (Feed f : feedList){
                System.out.println(f.getType() + " " + f.getId());
                if (!lastVersion.remove(f.getId())){
                    newFeed.add(f);
                }
            }
            lastVersion.clear();
            for (Feed f : feedList){
                lastVersion.add(f.getId());
            }
            copyToBackupBroker(new Message(MessageSender.BROKER, MessageAction.UPDATE_LAST_VERSION, lastFeedVersion));
            System.out.println("Get New Feed of " + producer.getType());
            System.out.println("==========BEGIN OF CURRENT BAG==========");
            if (!newFeed.isEmpty()) {
                for (Feed f : newFeed){
                    System.out.println(f.getType() + " " + f.getId());
                }
                System.out.println("===========END OF CURRENT BAG===========");
                new Thread(new NewFeedSender(newFeed)).start();
            }
            objectInputStream.close();
            socket.close();
        }

        private class NewFeedSender implements Runnable{

            private List<Feed> newFeed;

            private NewFeedSender(List<Feed> newFeed) {
                this.newFeed = newFeed;
            }

            public void run() {
                int idleServerIndex1 = -1, idleServerIndex2 = -1;
                long load1 = Integer.MAX_VALUE, load2 = Integer.MAX_VALUE;
                for (int i = 0; i < serverList.size(); i++){
                    if (serverLoad.get(serverList.get(i)) < load1 && serverList.get(i).isOn){
                        idleServerIndex1 = i;
                        load1 = serverLoad.get(serverList.get(i));
                    }
                    else if (serverLoad.get(serverList.get(i)) < load2 && serverList.get(i).isOn) {
                        idleServerIndex2 = i;
                        load2 = serverLoad.get(serverList.get(i));
                    }
                }
                if (idleServerIndex1 != -1){
                    if (!feeding(idleServerIndex1, newFeed)){
                        if (idleServerIndex2 != -1) {
                            System.out.println("Try Backup Server");
                            feeding(idleServerIndex2, newFeed);
                        }
                    }
                }
            }

            public boolean feeding(int idleServerIndex, List<Feed> newFeed){
                try{
                    System.out.println("Send New Feed to Server");
                    Socket socket = new Socket(serverList.get(idleServerIndex).getAddress(), serverList.get(idleServerIndex).getPortNumber());
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    Message messageSent = new Message(MessageSender.BROKER, MessageAction.NEW_FEED, newFeed);
                    objectOutputStream.writeObject(messageSent);
                    Message messageReceived = (Message) objectInputStream.readObject();
                    objectInputStream.close();
                    objectOutputStream.close();
                    socket.close();
                    if (messageReceived.getAction().equals(MessageAction.OK)){
                        System.out.println("Sending Done, Update Size");
                        serverLoad.put(serverList.get(idleServerIndex), (int) messageReceived.getContent());
                        List<String> newServerSize = new ArrayList<>();
                        newServerSize.add(serverList.get(idleServerIndex).getAddress());
                        newServerSize.add(Integer.toString(serverList.get(idleServerIndex).getPortNumber()));
                        newServerSize.add(Integer.toString((int) messageReceived.getContent()));
                        copyToBackupBroker(new Message(MessageSender.BROKER, MessageAction.NEW_SIZE, newServerSize));
                        return true;
                    }
                    return false;
                } catch (Exception e) {
                    System.out.println("Sending Error, Cannot Connect to Server");
                    e.printStackTrace();
                    serverList.get(idleServerIndex).setOn(false);
                    serverList.get(idleServerIndex).setNeedToCheck(true);
                    aliveServerList.remove(serverList.get(idleServerIndex));
                    List<String> parameter = new ArrayList<>();
                    parameter.add(serverList.get(idleServerIndex).getAddress());
                    parameter.add(Integer.toString(serverList.get(idleServerIndex).getPortNumber()));
                    copyToBackupBroker(new Message(MessageSender.BROKER, MessageAction.NEED_TO_CHECK, parameter));
                    return false;
                }
            }
        }
    }

    private class ServersChecker implements Runnable {

        private class ServerChecker implements Runnable {
            private Server server;

            private ServerChecker(Server server) {
                this.server = server;
            }

            public void run() {
                try{
                    System.out.println("Checking Server Status");
                    Socket socket = new Socket(this.server.getAddress(), this.server.getPortNumber());
                    if (socket.isConnected()){
                        System.out.println("Server Back On");
                        this.server.setOn(true);
                        this.server.setNeedToCheck(false);
                        boolean found = false;
                        for (Server s : aliveServerList) {
                            if (s.getPortNumber() == this.server.getPortNumber() && s.getAddress().equals(this.server.getAddress())){
                                found = true;
                                break;
                            }
                        }
                        if (!found){
                            aliveServerList.add(this.server);
                        }
                    }
                    socket.close();
                    List<String> parameter = new ArrayList<>();
                    parameter.add(this.server.getAddress());
                    parameter.add(Integer.toString(this.server.getPortNumber()));
                    copyToBackupBroker(new Message(MessageSender.BROKER, MessageAction.STOP_CHECKING, parameter));
                } catch (Exception e) {
                    System.out.println("Server Still Off");
                    e.printStackTrace();
                }
            }
        }

        public void run(){
            while (true){
                for (Server s : serverList){
                    if (s.needToCheck) {
                        new Thread(new ServerChecker(s)).start();
                    }
                }
                try {
                    Thread.sleep(1000 * 60 * 3);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class BackupHandler implements Runnable {

        public void run() {
            while (true){
                try {
                    System.out.println("Check If Main Broker Alive");
                    Socket socket = new Socket(BROKER_ADDRESS, brokerPortNumber);
                    socket.setSoTimeout(100);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.CHECK_IF_ALIVE, null));
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    objectInputStream.readObject();
                    objectOutputStream.close();
                    socket.close();
                    System.out.println("Main Broker Alive");
                } catch (Exception e) {
                    //e.printStackTrace();
                    try{
                        System.out.println("Main Broker Down, BackUp Broker Take Over");
                        brokerSocket.close();
                        brokerSocket = new ServerSocket(brokerPortNumber);
                        mainBroker = true;
                        if (!backupBrokerList.isEmpty()){
                            System.out.println("New BackUp Broker Standby");
                            BackupBroker backupBroker = backupBrokerList.get(0);
                            Socket socket = new Socket(backupBroker.getAddress(), backupBroker.getPortNumber());
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            objectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.RUN_AS_BACKUP, null));
                            objectOutputStream.close();
                            socket.close();
                        }
                        mainStart();
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                }
                try{
                    Thread.sleep(1000 * 10);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class RequestDistributor implements Runnable{
        private Socket socket;
        private ObjectInputStream objectInputStream;

        private RequestDistributor(Socket socket) throws Exception{
            this.socket = socket;
            this.objectInputStream = new ObjectInputStream(this.socket.getInputStream());
        }

        public void run(){
            try {
                Message newMessage = (Message) objectInputStream.readObject();
                String sender = newMessage.getSender();
                if (sender.equals(MessageSender.BROKER)){
                    Thread thread = new Thread(new BrokerCoordinator(this.socket, newMessage));
                    thread.start();
                }
                else if (sender.equals(MessageSender.SERVER)){
                    Thread thread = new Thread(new ServerCoordinator(this.socket, newMessage));
                    thread.start();
                }
                else if (sender.equals(MessageSender.CLIENT)){
                    Thread thread = new Thread(new ClientCoordinator(this.socket, newMessage));
                    thread.start();
                }
                else {
                    this.objectInputStream.close();
                    this.socket.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class BrokerCoordinator implements Runnable{
        private Socket socket;
        private Message message;

        private BrokerCoordinator(Socket socket, Message message) {
            this.socket = socket;
            this.message = message;
        }

        public void run() {
            System.out.println(this.message.getSender() + " Requires " + this.message.getAction());
            if (this.message.getAction().equals(MessageAction.ADD_SERVER)){
                addServer();
            }
            else if (this.message.getAction().equals(MessageAction.DEL_SERVER)){
                delServer();
            }
            else if (this.message.getAction().equals(MessageAction.NEW_SIZE)){
                updateSize();
            }
            else if (this.message.getAction().equals(MessageAction.ADD_CLIENT)){
                addClient();
            }
            else if (this.message.getAction().equals(MessageAction.DEL_CLIENT)){
                delClient();
            }
            else if (this.message.getAction().equals(MessageAction.ADD_BROKER)){
                addBroker();
            }
            else if (this.message.getAction().equals(MessageAction.CHECK_IF_ALIVE)){
                replyAlive();
            }
            else if (this.message.getAction().equals(MessageAction.RUN_AS_BACKUP)){
                runAsBackup();
            }
            else if (this.message.getAction().equals(MessageAction.NEW_TOPIC)){
                newTopic();
            }
            else if (this.message.getAction().equals(MessageAction.UPDATE_LAST_VERSION)){
                updateLastVersion();
            }
            else if (this.message.getAction().equals(MessageAction.NEED_TO_CHECK)){
                turnOnNeedToCheck();
            }
            else if (this.message.getAction().equals(MessageAction.STOP_CHECKING)){
                turnoffNeedToCheck();
            }
            try {
                this.socket.close();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

        private void addServer() {
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int port = Integer.parseInt(content.get(1));
            boolean foundRecord = false;
            Server recordServer = null;
            for (Server s : serverList) {
                if (s.getAddress().equals(address) && s.getPortNumber() == port){
                    s.setOn(true);
                    s.setNeedToCheck(false);
                    foundRecord = true;
                    recordServer = s;
                    int index = -1;
                    for (int i = 0; i < aliveServerList.size(); i++){
                        if (aliveServerList.get(i).getAddress().equals(address) && aliveServerList.get(i).getPortNumber() == port){
                            index = i;
                            break;
                        }
                    }
                    if (index != -1) {
                        aliveServerList.remove(s);
                    }
                }
            }
            Server server = foundRecord ? recordServer : new Server(content.get(0), Integer.parseInt(content.get(1)), true, false);
            if (!foundRecord){
                serverList.add(server);
                serverLoad.put(server, 0);
            }
            aliveServerList.add(server);
        }

        private void delServer() {
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int portNumber = Integer.parseInt(content.get(1));
            for (Server s : serverList){
                if (s.getAddress().equals(address) && s.getPortNumber() == portNumber){
                    s.setOn(false);
                    s.setNeedToCheck(false);
                    break;
                }
            }
            int delIndex = -1;
            for (int i = 0; i < aliveServerList.size(); i++){
                if (aliveServerList.get(i).getAddress().equals(address) && aliveServerList.get(i).getPortNumber() == portNumber){
                    delIndex = i;
                    break;
                }
            }
            aliveServerList.remove(delIndex);
        }

        private void updateSize(){
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int portNumber = Integer.parseInt(content.get(1));
            int newSize = Integer.parseInt(content.get(2));
            for (Server s : serverList){
                if (s.getAddress().equals(address) && s.getPortNumber() == portNumber){
                    serverLoad.put(s, newSize);
                    break;
                }
            }
        }

        private void addClient() {
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int portNumber = Integer.parseInt(content.get(1));
            String type = content.get(2);
            if (!subscribeList.containsKey(type)) {
                subscribeList.put(type, new ArrayList<>());
            }
            List<Client> subscriber = subscribeList.get(type);
            boolean found = false;
            for (Client c : subscriber){
                if (c.getAddress().equals(address) && c.getPortNumber() == portNumber){
                    c.setOn(true);
                    found = true;
                    break;
                }
            }
            if (!found){
                Client client = new Client(address, portNumber, true);
                subscribeList.get(type).add(client);
            }

        }

        private void delClient() {
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int portNumber = Integer.parseInt(content.get(1));
            String type = content.get(2);
            if (!subscribeList.containsKey(type)){
                return;
            }
            List<Client> subscriber = subscribeList.get(type);
            for (Client c : subscriber){
                if (c.getAddress().equals(address) && c.getPortNumber() == portNumber){
                    c.setOn(false);
                    break;
                }
            }
        }

        private void addBroker() {
            copyToBackupBroker(this.message);
            List<String> backupBrokerParameter = (List<String>) this.message.getContent();
            BackupBroker backupBroker = new BackupBroker(backupBrokerParameter.get(0), Integer.parseInt(backupBrokerParameter.get(1)), true);
            if (mainBroker && backupBrokerList.isEmpty()){
                try{
                    Socket socket = new Socket(backupBroker.getAddress(), backupBroker.getPortNumber());
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.RUN_AS_BACKUP, null));
                    objectOutputStream.close();
                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            backupBrokerList.add(backupBroker);
        }

        private void replyAlive() {
            try{
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
                objectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.ALIVE, null));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void runAsBackup() {
            new Thread(backupHandler).start();
        }

        private void newTopic() {
            List<String> parameters = (List<String>) this.message.getContent();
            Producer newProducer = new Producer(parameters.get(2), parameters.get(0), Integer.parseInt(parameters.get(1)), true);
            producerList.add(newProducer);
            lastFeedVersion.put(parameters.get(2), new HashSet<>());
        }

        private void updateLastVersion(){
            lastFeedVersion = (Map<String, Set<String>>)this.message.getContent();
        }

        private void turnOnNeedToCheck() {
            List<String> parameter = (List<String>) this.message.getContent();
            String address = parameter.get(0);
            int port = Integer.parseInt(parameter.get(1));
            for (Server s : serverList) {
                if (s.getAddress().equals(address) && s.getPortNumber() == port){
                    s.setNeedToCheck(true);
                    s.setOn(false);
                }
            }
            int index = -1;
            for (int i = 0; i < aliveServerList.size(); i++) {
                if (aliveServerList.get(i).getAddress().equals(address) && aliveServerList.get(i).getPortNumber() == port){
                    index = i;
                }
            }
            if (index != -1) {
                aliveServerList.remove(index);
            }
        }

        private void turnoffNeedToCheck() {
            List<String> parameter = (List<String>) this.message.getContent();
            String address = parameter.get(0);
            int port = Integer.parseInt(parameter.get(1));
            for (Server s : serverList) {
                if (s.getAddress().equals(address) && s.getPortNumber() == port){
                    s.setNeedToCheck(false);
                    s.setOn(true);
                    boolean found = false;
                    for (Server s1 : aliveServerList) {
                        if (s1.getPortNumber() == port && s1.getAddress().equals(address)){
                            found = true;
                            break;
                        }
                    }
                    if (!found){
                        aliveServerList.add(s);
                    }
                    break;
                }
            }
        }
    }

    private class ServerCoordinator implements Runnable{
        private Socket socket;
        private Message message;
        private ObjectOutputStream totalObjectOutputStream;

        private ServerCoordinator(Socket socket, Message message) {
            this.socket = socket;
            this.message = message;
            try {
                this.totalObjectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void run() {
            System.out.println(this.message.getSender() + " Requires " + this.message.getAction());
            if (this.message.getAction().equals(MessageAction.ADD_SERVER)){
                addServer();
            }
            else if (this.message.getAction().equals(MessageAction.DEL_SERVER)){
                delServer();
            }
            else if (this.message.getAction().equals(MessageAction.NEW_SIZE)){
                updateSize();
            }
            else if (this.message.getAction().equals(MessageAction.ADD_FOLLOWER)){
                addFollower(false);
            }
            try {
                this.totalObjectOutputStream.close();
                this.socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void addServer() {
            copyToBackupBroker(this.message);
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int port = Integer.parseInt(content.get(1));
            boolean foundRecord = false;
            Server recordServer = null;
            for (Server s : serverList) {
                if (s.getAddress().equals(address) && s.getPortNumber() == port){
                    s.setNeedToCheck(false);
                    s.setOn(true);
                    foundRecord = true;
                    recordServer = s;
                    int index = -1;
                    for (int i = 0; i < aliveServerList.size(); i++){
                        if (aliveServerList.get(i).getAddress().equals(address) && aliveServerList.get(i).getPortNumber() == port){
                            index = i;
                            break;
                        }
                    }
                    if (index != -1) {
                        aliveServerList.remove(s);
                    }
                }
            }
            Server server = foundRecord ? recordServer : new Server(content.get(0), Integer.parseInt(content.get(1)), true, false);
            if (!foundRecord){
                serverList.add(server);
                serverLoad.put(server, 0);
            }
            addFollower(true);
            aliveServerList.add(server);
            if (serverList.size() == 2){
                try{
                    Socket newSocket = new Socket(serverList.get(0).getAddress(), serverList.get(0).getPortNumber());
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(newSocket.getOutputStream());
                    HashMap<String, List<String>> followerList = getNewFollower(true);
                    objectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.ADD_FOLLOWER, followerList));
                    objectOutputStream.close();
                    newSocket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void addFollower(boolean newServer) {
            if (!aliveServerList.isEmpty()){
                try {
                    HashMap<String, List<String>> followerList = getNewFollower(false);
                    totalObjectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.ADD_FOLLOWER, followerList));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
                try {
                    totalObjectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.NO_AVA_SERVER, null));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (newServer){
                HashMap<String, List<List<String>>> clientList = new HashMap<>();
                for (String s : subscribeList.keySet()){
                    List<List<String>> clients = new ArrayList<>();
                    for (Client c : subscribeList.get(s)){
                        if (c.isOn()){
                            List<String> client = new ArrayList<>();
                            client.add(c.getAddress());
                            client.add(Integer.toString(c.getPortNumber()));
                            client.add(BROKER_ADDRESS);
                            client.add(Integer.toString(brokerPortNumber));
                            clients.add(client);
                        }
                    }
                    clientList.put(s, clients);
                }
                try {
                    totalObjectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.ADD_CLIENT_LIST, clientList));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void delServer() {
            copyToBackupBroker(this.message);
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int portNumber = Integer.parseInt(content.get(1));
            for (Server s : serverList){
                if (s.getAddress().equals(address) && s.getPortNumber() == portNumber){
                    s.setOn(false);
                    s.setNeedToCheck(false);
                    break;
                }
            }
            int delIndex = -1;
            for (int i = 0; i < aliveServerList.size(); i++){
                if (aliveServerList.get(i).getAddress().equals(address) && aliveServerList.get(i).getPortNumber() == portNumber){
                    delIndex = i;
                    break;
                }
            }
            aliveServerList.remove(delIndex);
            replyOK(this.totalObjectOutputStream);
        }

        private void updateSize(){
            copyToBackupBroker(this.message);
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int portNumber = Integer.parseInt(content.get(1));
            int newSize = Integer.parseInt(content.get(2));
            for (Server s : serverList){
                if (s.getAddress().equals(address) && s.getPortNumber() == portNumber){
                    serverLoad.put(s, newSize);
                    break;
                }
            }
            replyOK(this.totalObjectOutputStream);
        }

    }

    private class ClientCoordinator implements Runnable{

        private class BroadCaster implements Runnable{
            private Server server;
            private Message message;

            private BroadCaster(Server server, Message message) {
                this.server = server;
                this.message = message;
            }

            public void run() {
                try {
                    Socket newSocket = new Socket(this.server.getAddress(), this.server.getPortNumber());
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(newSocket.getOutputStream());
                    objectOutputStream.writeObject(message);
                    objectOutputStream.close();
                    newSocket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }

        private Socket socket;
        private Message message;
        private ObjectOutputStream totalObjectOutputStream;

        private ClientCoordinator(Socket socket, Message message) {
            this.socket = socket;
            this.message = message;
            try {
                this.totalObjectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void run() {
            System.out.println(this.message.getSender() + " Requires " + this.message.getAction());
            if (this.message.getAction().equals(MessageAction.ADD_CLIENT)){
                addClient();
            }
            else if (this.message.getAction().equals(MessageAction.DEL_CLIENT)){
                delClient();
            }
            try {
                this.totalObjectOutputStream.close();
                this.socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void addClient() {
            copyToBackupBroker(this.message);
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int portNumber = Integer.parseInt(content.get(1));
            String type = content.get(2);
            if (!subscribeList.containsKey(type)) {
                subscribeList.put(type, new ArrayList<>());
                new Thread(new ProducerCaller(true, type)).start();
                if (serverList.size() > 1){
                    for (Server s : serverList){
                        List<Server> followerCandidate = new ArrayList<>();
                        for (Server s1 : aliveServerList){
                            if (s1.getAddress().equals(s.getAddress()) && s1.getPortNumber() == s.getPortNumber()){
                                continue;
                            } else {
                                followerCandidate.add(s1);
                            }
                        }
                        HashMap<String, List<String>> followerList = new HashMap<>();
                        Random random = new Random();
                        for (String topic : subscribeList.keySet()){
                            List<String> socketInformation = new ArrayList<>();
                            int randomNumber = random.nextInt(followerCandidate.size());
                            Server follower = followerCandidate.get(randomNumber);
                            socketInformation.add(follower.getAddress());
                            socketInformation.add(Integer.toString(follower.getPortNumber()));
                            followerList.put(topic, socketInformation);
                        }
                        try{
                            Socket socket = new Socket(s.getAddress(), s.getPortNumber());
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            objectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.ADD_FOLLOWER, followerList));
                            objectOutputStream.close();
                            socket.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            List<Client> subscriber = subscribeList.get(type);
            boolean found = false;
            boolean needToSend = false;
            for (Client c : subscriber){
                if (c.getAddress().equals(address) && c.getPortNumber() == portNumber){
                    if (!c.isOn){
                        needToSend = true;
                    }
                    c.setOn(true);
                    found = true;
                    break;
                }
            }
            if (!found){
                needToSend = true;
                Client client = new Client(address, portNumber, true);
                subscribeList.get(type).add(client);
            }
            if (needToSend) {
                List<String> newClient = new ArrayList<>();
                newClient.add(address);
                newClient.add(Integer.toString(portNumber));
                newClient.add(BROKER_ADDRESS);
                newClient.add(Integer.toString(brokerPortNumber));
                newClient.add(type);
                Message newMessage = new Message(MessageSender.BROKER, MessageAction.ADD_CLIENT, newClient);
                for (Server s : aliveServerList){
                    new Thread(new BroadCaster(s, newMessage)).run();
                }
            }
            replyOK(this.totalObjectOutputStream);
        }

        private void delClient() {
            copyToBackupBroker(this.message);
            List<String> content = (List<String>) this.message.getContent();
            String address = content.get(0);
            int portNumber = Integer.parseInt(content.get(1));
            String type = content.get(2);
            if (!subscribeList.containsKey(type)){
                try {
                    this.totalObjectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.NOT_THIS_TYPE, null));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            List<Client> subscriber = subscribeList.get(type);
            boolean needToSend = false;
            for (Client c : subscriber){
                if (c.getAddress().equals(address) && c.getPortNumber() == portNumber){
                    needToSend = true;
                    c.setOn(false);
                    break;
                }
            }
            if (needToSend) {
                List<String> messageContent = new ArrayList<>();
                messageContent.add(address);
                messageContent.add(Integer.toString(portNumber));
                messageContent.add(BROKER_ADDRESS);
                messageContent.add(Integer.toString(brokerPortNumber));
                messageContent.add(type);
                Message newMessage = new Message(MessageSender.BROKER, MessageAction.DEL_CLIENT, messageContent);
                for (Server s : aliveServerList){
                    new Thread(new BroadCaster(s, newMessage)).run();
                }
            }
            replyOK(this.totalObjectOutputStream);
        }
    }

    private Broker() throws Exception{

        this.producerCaller = new ProducerCaller(false, "");
        this.serversChecker = new ServersChecker();
        this.backupHandler = new BackupHandler();
        this.lastFeedVersion = new ConcurrentHashMap<>();
        this.subscribeList = new ConcurrentHashMap<>();
        this.serverList = new CopyOnWriteArrayList<>();
        this.aliveServerList = new CopyOnWriteArrayList<>();
        this.serverLoad = new ConcurrentHashMap<>();
        this.backupBrokerList = new CopyOnWriteArrayList<>();
        this.producerList = new CopyOnWriteArrayList<>();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));

       //System.out.println("Please Input IP Address of Producer");
        this.producerAddress = "127.0.0.1";   //bufferedReader.readLine();
       //System.out.println("Please Input Port Number of Producer");
        this.producerPortNumber = 8001; //Integer.parseInt(bufferedReader.readLine());
        System.out.println("Main Broker? (Y/N)");
        String answer = bufferedReader.readLine();
        this.mainBroker = (answer.equals("Y"));
        if (mainBroker) {
            //System.out.println("Please Input Broker Port Number:");
            this.brokerPortNumber = 7000;//Integer.parseInt(bufferedReader.readLine());
            this.brokerSocket = new ServerSocket(brokerPortNumber);
            mainStart();
        }
        else {
            System.out.println("Please Input Backup Broker Port Number:");
            int backupBrokerPortNumber = Integer.parseInt(bufferedReader.readLine());
            this.brokerSocket = new ServerSocket(backupBrokerPortNumber);
            //System.out.println("Please Input Main Broker Port Number:");
            this.brokerPortNumber = 7000; //Integer.parseInt(bufferedReader.readLine());
            Socket socket = new Socket(BROKER_ADDRESS, brokerPortNumber);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            List<String> backupBrokerParameter = new ArrayList<>();
            backupBrokerParameter.add(BROKER_ADDRESS);
            backupBrokerParameter.add(Integer.toString(backupBrokerPortNumber));
            Message message = new Message(MessageSender.BROKER, MessageAction.ADD_BROKER, backupBrokerParameter);
            objectOutputStream.writeObject(message);
            objectOutputStream.close();
            socket.close();
            backupStart();
        }
        bufferedReader.close();
    }

    private void mainStart() throws Exception{
        System.out.println("Main Broker Start");
        new Thread(this.producerCaller).start();
        new Thread(this.serversChecker).start();
        while(true){
            Socket inboundSocket = this.brokerSocket.accept();
            Thread thread = new Thread(new RequestDistributor(inboundSocket));
            thread.start();
        }
    }

    private void backupStart() {
        System.out.println("Backup Broker Standby");
        try{
            while(true){
                Socket inboundSocket = this.brokerSocket.accept();
                Thread thread = new Thread(new RequestDistributor(inboundSocket));
                thread.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void replyOK(ObjectOutputStream objectOutputStream) {
        try{
            objectOutputStream.writeObject(new Message(MessageSender.BROKER, MessageAction.OK, null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void copyToBackupBroker(Message message) {

        class Copier implements Runnable{

            private BackupBroker backupBroker;

            private Copier(BackupBroker backupBroker) {
                this.backupBroker = backupBroker;
            }

            public void run() {
                try{
                    Socket socket = new Socket(this.backupBroker.getAddress(), this.backupBroker.getPortNumber());
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(new Message(MessageSender.BROKER, message.getAction(), message.getContent()));
                    objectOutputStream.close();
                    socket.close();
                    System.out.println("Copy to Backup Broker");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        for (BackupBroker b : this.backupBrokerList){
            new Thread(new Copier(b)).run();
        }
    }

    private HashMap<String, List<String>> getNewFollower(boolean firstServer) {
        HashMap<String, List<String>> followerList = new HashMap<>();
        Random random = new Random();
        for (String topic : subscribeList.keySet()){
            List<String> socketInformation = new ArrayList<>();
            int randomNumber = firstServer ? 1 : random.nextInt(aliveServerList.size());
            Server follower = aliveServerList.get(randomNumber);
            socketInformation.add(follower.getAddress());
            socketInformation.add(Integer.toString(follower.getPortNumber()));
            followerList.put(topic, socketInformation);
        }
        return followerList;
    }

    public static void main(String[] args){
        try{
            new Broker();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
