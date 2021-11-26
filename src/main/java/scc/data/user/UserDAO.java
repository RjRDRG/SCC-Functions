package scc.data.user;

import java.util.Arrays;

/**
 * Represents a User, as stored in the database
 */
public class UserDAO {
	private String _rid;
	private String _ts;
	private String idUser;
	private String name;
	private String pwd;
	private String photoId;
	private String[] channelIds;

	public UserDAO() {
	}
	public UserDAO( User u) {
		this(u.getIdUser(), u.getName(), u.getPwd(), u.getPhotoId(), u.getChannelIds());
	}
	
	public UserDAO(String idUser, String name, String pwd, String photoId, String[] channelIds) {
		super();
		this.idUser = idUser;
		this.name = name;
		this.pwd = pwd;
		this.photoId = photoId;
		this.channelIds = channelIds;
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
	public String getIdUser() {
		return idUser;
	}
	public void setIdUser(String idUser) {
		this.idUser = idUser;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPwd() {
		return pwd;
	}
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}
	public String getPhotoId() {
		return photoId;
	}
	public void setPhotoId(String photoId) {
		this.photoId = photoId;
	}
	public String[] getChannelIds() {
		return channelIds == null ? new String[0] : channelIds ;
	}
	public void setChannelIds(String[] channelIds) {
		this.channelIds = channelIds;
	}
	public User toUser() {
		return new User(idUser, name, pwd, photoId, channelIds == null ? null : Arrays.copyOf(channelIds,channelIds.length));
	}
	@Override
	public String toString() {
		return "UserDAO [_rid=" + _rid + ", _ts=" + _ts + ", idUser=" + idUser + ", name=" + name + ", pwd=" + pwd
				+ ", photoId=" + photoId + ", channelIds=" + Arrays.toString(channelIds) + "]";
	}

}
