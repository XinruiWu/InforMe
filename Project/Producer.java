package Project;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.*;

public class Producer {
    private static final int FEED_GENERATOR_TIME_INTERVAL = 10000; //generate every 10 sec;
    private static final int FEED_EXPIROR_TIME_INTERVAL = 40000; // expire every 40 sec;
    private static final int FEED_EXPIROR_DIFF = 35000;
    private static final String dic = "ABCDEFGHIJKLMNOPQRSTUVWXYZ-abcdefghijklmnopqrstuvwxyz-1234567890";

    private ServerSocket serverSocket;
    private int portNumber;
    private String producerType;
    private Vector<Feed> feedList;
    private FeedGenerator feedGenerator;
    private FeedExpiror feedExpiror;
    private volatile int feedId;

    public Producer(int portNumber, String feedType) throws Exception {
        this.portNumber = portNumber;
        this.producerType = feedType;
        this.serverSocket = new ServerSocket(portNumber);
        this.feedList = new Vector<>();
        this.feedGenerator = new FeedGenerator();
        this.feedExpiror = new FeedExpiror();
        this.feedId = 0;
    }

    void setFeedId(int newId) {
        this.feedId = newId;
    }

    void start() throws Exception{
        new Thread(feedGenerator).start();
        new Thread(feedExpiror).start();
        respondToBroker(producerType);
    }

    private class FeedGenerator implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    generateFeed(producerType);
                    Thread.sleep(FEED_GENERATOR_TIME_INTERVAL);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void generateFeed(String type) {
            String id = generateId();
            String content = generateContent();
            feedList.add(new Feed(id, type, content, new Timestamp(System.currentTimeMillis())));
            System.out.println("Producer generates: " + id + " " + type);
        }

        private String generateId() {
            int curId = Producer.this.feedId + 1;
            StringBuilder stringBuilder = new StringBuilder();
            if(stringBuilder.length() < 5) {
                int diff = 5 - stringBuilder.length();
                for(int i = 0; i < diff; i ++) {
                    stringBuilder.append(0);
                }
                stringBuilder.append(curId);
            }
            setFeedId(Producer.this.feedId + 1);
            return stringBuilder.toString();
        }

        private String generateContent() {
            StringBuilder stringBuilder = new StringBuilder();
            Random random = new Random();
            while(stringBuilder.length() < 10) {
                int index = (int) (random.nextFloat() * dic.length());
                stringBuilder.append(dic.charAt(index));
            }
            return stringBuilder.toString();
        }
    }

    private class FeedExpiror implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    expireFeed(producerType);
                    Thread.sleep(FEED_EXPIROR_TIME_INTERVAL);
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

        //Expire feed generated 30 seconds ago
        private void expireFeed(String type) {
            if(feedList.size() == 0 || !feedList.get(0).getType().equals(producerType)) {
                return;
            }
            long targetTime = new Timestamp(System.currentTimeMillis()).getTime() - FEED_EXPIROR_DIFF;
            Timestamp targetExpireTimestamp = new Timestamp(targetTime);
            int targetTimeIndex = findExpireIndex(targetExpireTimestamp);
            if(targetTimeIndex == -1) return;
            for(int i = 0; i <= targetTimeIndex; i++) {
                feedList.remove(0);
            }
        }

        private int findExpireIndex(Timestamp targetTimestamp) {
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

    private void respondToBroker(String producerType) throws Exception {
        while(true) {
            Socket brokerSocket = this.serverSocket.accept();
            ObjectOutputStream out = new ObjectOutputStream(brokerSocket.getOutputStream());
            List<Feed> res = new ArrayList<>(feedList);

            System.out.println("Producer sends " + res.size() + " feeds to Broker");
            for(Feed f: res) {
                System.out.println("Feed: " + f.getType() + " " + f.getId());
            }
            out.writeObject(res);
        }
    }
}
