package Project;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ServerThread extends Thread {
	private Socket socket;
	private Server server;

	public ServerThread(Server server, Socket socket) {
		this.server = server;
		this.socket = socket;
	}

	@Override
	public void run() {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			Message m = (Message) ois.readObject();

			if(m.getAction().equals(MessageAction.ADD_CLIENT)) {
				List<String> list = (List<String>) m.getContent();
				Client client = new Client(list.get(0), Integer.parseInt(list.get(1)), list.get(2), Integer.parseInt(list.get(3)));
				String type = (String) list.get(4);
				server.addClients(client, type);
			}
			else if(m.getAction().equals(MessageAction.DEL_CLIENT)) {
				List<String> list = (List<String>) m.getContent();
				Client client = new Client(list.get(0), Integer.parseInt(list.get(1)), list.get(2), Integer.parseInt(list.get(3)));
				String type = (String) list.get(4);
				server.deleteClient(client, type);
			}
			else if(m.getAction().equals(MessageAction.NEW_FEED)) {
				List<Feed> feeds = (List<Feed>) m.getContent();
				System.out.println("==================================================================");
				System.out.println("Copy Feeds from " + m.getSender());
				this.server.addFeeds(feeds);
				if(m.getSender().equals(MessageSender.SERVER)) {
				}
				if(m.getSender().equals(MessageSender.BROKER)) {
					this.server.sendCopyToFollower(feeds);
					this.server.broadCast(feeds, false);
				}
				int new_size = this.server.getPartitionSize();
				Message m2 = new Message(MessageSender.SERVER, MessageAction.OK, new_size);
				oos.writeObject(m2);
			} else if(m.getAction().equals(MessageAction.ADD_FOLLOWER)) {
				this.server.setFollowers((HashMap<String, List<String>>) m.getContent());
				this.server.initialFollowers();
			} else if(m.getAction().equals(MessageAction.FOLLOWER_SEND) && m.getSender().equals(MessageSender.SERVER)) {
				System.out.println("Act as follower to send feeds to clients");
				ArrayList<Feed> feeds = (ArrayList<Feed>) m.getContent();
				this.server.broadCast(feeds, true);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Socket getSocket() {
		return socket;
	}

}

