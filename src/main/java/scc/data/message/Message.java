package scc.data.message;

public class Message {
	private String idMessage;
	private String send;
	private String dest;
	private String text;
	private String idPhoto;
	private String channel;
	private String replied;

	public Message(String idMessage, String dest, String send, String text, String idPhoto, String channel, String replied) {
		super();
		this.idMessage = idMessage;
		this.dest = dest;
		this.send = send;
		this.text = text;
		this.idPhoto = idPhoto;
		this.replied = replied;
		this.channel = channel;
	}

	public String getIdMessage() {
		return idMessage;
	}

	public void setIdMessage(String idMessage) {
		this.idMessage = idMessage;
	}

	public String getSend() {
		return send;
	}

	public void setSend(String send) {
		this.send = send;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
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
				"idMessage='" + idMessage + '\'' +
				", send='" + send + '\'' +
				", dest='" + dest + '\'' +
				", text='" + text + '\'' +
				", idPhoto='" + idPhoto + '\'' +
				", channel='" + channel + '\'' +
				", replied='" + replied + '\'' +
				'}';
	}
}
