package data.trending;

public class TrendingChannelDAO {

    private String _rid;
    private String _ts;
    private String id;
    private Long count;

    public TrendingChannelDAO() {
    }

    public TrendingChannelDAO(TrendingChannel trendingChannel) {
        this.id = trendingChannel.getId();
        this.count = trendingChannel.getCount();
    }

    public TrendingChannelDAO(String id, Long count) {
        super();
        this.id = id;
        this.count = count;
    }

    public TrendingChannel toTrendingChannel() {
        return new TrendingChannel(this.id,this.count);
    }

    public String get_rid() {
        return _rid;
    }

    public void set_rid(String _rid) {
        this._rid = _rid;
    }

    public String get_ts() {
        return _ts;
    }

    public void set_ts(String _ts) {
        this._ts = _ts;
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
                "_rid='" + _rid + '\'' +
                ", _ts='" + _ts + '\'' +
                ", id='" + id + '\'' +
                ", count=" + count +
                '}';
    }
}