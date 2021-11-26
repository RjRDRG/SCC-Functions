package scc.data.message;

public class MessageDAO {

	private String _rid;
	private String _ts;
	private String idMessage;
	private String send;
	private String dest;
	private String text;
	private String idPhoto;
	private String channel;
	private String replied;

	public MessageDAO() {
	}

	public MessageDAO(Message msg) {
		this(msg.getIdMessage(), msg.getSend(), msg.getDest(), msg.getText(), msg.getIdPhoto(), msg.getChannel(),
				msg.getReplied());
	}

	public MessageDAO(String idMessage, String send, String dest, String text, String idPhoto, String channel,
					  String replied) {
		super();
		this.idMessage = idMessage;
		this.send = send;
		this.dest = dest;
		this.text = text;
		this.idPhoto = idPhoto;

		this.replied = replied;
		this.channel = channel;
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

	public String getIdMessage() {
		return idMessage;
	}

	public void setIdMessage(String idMessage) {
		this.idMessage = idMessage;
	}

	public String getsend() {
		return send;
	}

	public void setsend(String send) {
		this.send = send;
	}

	public String getdest() {
		return dest;
	}

	public void setdest(String dest) {
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

	public void setIdPhoto(String id) {
		this.idPhoto = id;
	}

	public Message toMessage() {
		return new Message(idMessage, send, dest, text, idPhoto, channel, replied);
	}

	public String getReplied() {
		return replied;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public void setReplied(String replied) {
		this.replied = replied;
	}

	@Override
	public String toString() {
		return "MessageDAO [_rid=" + _rid + ", _ts=" + _ts + ", idMessage=" + idMessage + ", send=" + send + ", dest=" + dest
				+ "idPhoto=" + idPhoto + "]";
	}

}
