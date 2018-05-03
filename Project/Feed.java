package Project;
import java.io.Serializable;
import java.sql.Timestamp;

public class Feed implements Serializable{
    private String id;
    private String type;
    private String content;
    private Timestamp timestamp;

    public Feed(String id, String type, String content, Timestamp timestamp){
        this.id = id;
        this.type = type;
        this.content = content;
        this.timestamp = timestamp;
    }

    public String getId() {
        return this.id;
    }

    public String getType() {
        return this.type;
    }

    public String getContent() {
        return this.content;
    }

    public Timestamp getTimestamp() {
        return this.timestamp;
    }
}
