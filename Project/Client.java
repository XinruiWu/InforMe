package Project;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Client {
	private String address;
	private int port;
	private ServerSocket serverSocket;
	private int broker_port;
	private String broker_address;

	public Client(String address, int port, String broker_address, int broker_port) throws IOException {
		this.address = address;
		this.port = port;
		this.broker_port = broker_port;
		this.broker_address = broker_address;
	}

	@Override
	public String toString() {
		return "Client [address=" + address + ", port=" + port + "]";
	}

	public void register(String type) {
		List<String> content = new ArrayList<>();
		content.add(address);
		content.add(String.valueOf(port));
		content.add(type);

		Message m = new Message(MessageSender.CLIENT, MessageAction.ADD_CLIENT, content);
		try {
			Socket socket = new Socket(address, broker_port);
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(m);
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void start() throws IOException {
		serverSocket = new ServerSocket(port);
		while (true){
			Socket socket = serverSocket.accept();
			ClientThread clientThread = new ClientThread(socket);
			clientThread.start();
		}
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	public String getBroker_address() {
		return broker_address;
	}

}