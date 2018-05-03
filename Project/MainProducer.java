package Project;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


public class MainProducer {
    private static final int PRODUCER_PORT_NUMBER = 8001;
    private static final int PORT_NUMBER_GENERATE_BASE = 8002;
    private static final String PRODUCER_ADDRESS = "127.0.0.1";

    private int portNumber;
    private ServerSocket serverSocket;
    private Vector<String> typeVector;
    private Vector<Integer> portNumberVector;

    public MainProducer(int portNumber) throws Exception {
        this.portNumber = portNumber;
        this.serverSocket = new ServerSocket(portNumber);
        this.typeVector = new Vector<>();
        this.portNumberVector = new Vector<>();
    }

    private void startExecute() {
        while(true) {
            try {
                Socket brokerSocket = this.serverSocket.accept();
                ObjectInputStream in = new ObjectInputStream(brokerSocket.getInputStream());
                Message brokerMessage = (Message) in.readObject();
                if (brokerMessage.getSender().equals(MessageSender.BROKER) && brokerMessage.getAction().equals(MessageAction.NEW_TOPIC)) {
                    String type = (String) brokerMessage.getContent();
                    this.typeVector.add(type);
                    int portNumber = portNumberVector.size() + PORT_NUMBER_GENERATE_BASE; //port number is generated in sequence
                    this.portNumberVector.add(portNumber);
                    new Thread(new ProducerCreator(type, portNumber, brokerSocket)).start();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class ProducerCreator implements Runnable {
        private String newType;
        private int portNumber;
        private Socket socket;
        public ProducerCreator(String type, int portNumber, Socket brokerSocket) {
            this.newType = type;
            this.portNumber = portNumber;
            this.socket = brokerSocket;
        }
        @Override
        public void run() {
            try {
                Producer newProducer = new Producer(portNumber, newType);
                System.out.println("==================================================================");
                System.out.println("Generate new Producer on port " + portNumber + " for " + newType);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                List<String> content = new ArrayList<>();
                content.add(PRODUCER_ADDRESS);
                content.add(Integer.toString(this.portNumber));
                out.writeObject(new Message(MessageSender.PRODUCER, MessageAction.NEW_TOPIC, content));
                out.close();
                socket.close();

                newProducer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println("MainProducer ready on port " + PRODUCER_PORT_NUMBER);
            MainProducer mainProducer = new MainProducer(PRODUCER_PORT_NUMBER);
            mainProducer.startExecute();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}
