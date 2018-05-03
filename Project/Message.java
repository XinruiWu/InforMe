package Project;
import java.io.Serializable;

public class Message implements Serializable {

    private String sender;
	private String action;
	private Object content;

	public Message(String sender, String action, Object content) {
	    this.sender = sender;
	    this.action = action;
	    this.content = content;
    }

    public String getSender() {
        return sender;
    }

    public String getAction() {
        return action;
    }

    public Object getContent() {
        return content;
    }
}