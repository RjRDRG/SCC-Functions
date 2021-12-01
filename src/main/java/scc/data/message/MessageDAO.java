package scc.data.message;

public class MessageDAO {

	private String _rid;
	private String _ts;
	private String id;
	private String user;
	private String text;
	private String idPhoto;
	private String channel;
	private String replied;

	public MessageDAO() {}

	public MessageDAO(Message msg) {
		this(msg.getId(), msg.getUser(), msg.getText(), msg.getIdPhoto(), msg.getChannel(), msg.getReplied());
	}

	public MessageDAO(String id, String user, String text, String idPhoto, String channel, String replied) {
		super();
		this.id = id;
		this.user = user;
		this.text = text;
		this.idPhoto = idPhoto;

		this.replied = replied;
		this.channel = channel;
	}

	public Message toMessage() {
		return new Message(id, user, text, idPhoto, channel, replied);
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

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getIdPhoto() {
		return idPhoto;
	}

	public void setIdPhoto(String idPhoto) {
		this.idPhoto = idPhoto;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getReplied() {
		return replied;
	}

	public void setReplied(String replied) {
		this.replied = replied;
	}


	@Override
	public String toString() {
		return "MessageDAO{" +
				"_rid='" + _rid + '\'' +
				", _ts='" + _ts + '\'' +
				", id='" + id + '\'' +
				", user='" + user + '\'' +
				", text='" + text + '\'' +
				", idPhoto='" + idPhoto + '\'' +
				", channel='" + channel + '\'' +
				", replied='" + replied + '\'' +
				'}';
	}
}
