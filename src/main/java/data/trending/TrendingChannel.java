package data.trending;

public class TrendingChannel {

    private String id;
    private Long count;

    public TrendingChannel() {
    }

    public TrendingChannel(String id, Long count) {
        this.id = id;
        this.count = count;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "TrendingChannelDAO{" +
                "id='" + id + '\'' +
                ", count=" + count +
                '}';
    }
}