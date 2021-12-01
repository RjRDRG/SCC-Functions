package scc.data.message;

public class Message {
	private String id;
	private String user;
	private String text;
	private String idPhoto;
	private String channel;
	private String replied;

	public Message() {
	}

	public Message(String id, String user, String text, String idPhoto, String channel, String replied) {
		super();
		this.id = id;
		this.user = user;
		this.text = text;
		this.idPhoto = idPhoto;
		this.replied = replied;
		this.channel = channel;
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
		return "Message{" +
				"id='" + id + '\'' +
				", user='" + user + '\'' +
				", text='" + text + '\'' +
				", idPhoto='" + idPhoto + '\'' +
				", channel='" + channel + '\'' +
				", replied='" + replied + '\'' +
				'}';
	}
}
