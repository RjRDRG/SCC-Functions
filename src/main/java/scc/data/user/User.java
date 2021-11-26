package scc.data.user;

import java.util.Arrays;

/**
 * Represents a User, as returned to the clients
 */
public class User {
	private String idUser;
	private String name;
	private String pwd;
	private String photoId;
	private String[] channelIds;

	public User(String idUser, String name, String pwd, String photoId, String[] channelIds) {
		super();
		this.idUser = idUser;
		this.name = name;
		this.pwd = pwd;
		this.photoId = photoId;
		this.channelIds = channelIds;
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
		return channelIds == null ? new String[0] : channelIds;
	}

	public void setChannelIds(String[] channelIds) {
		this.channelIds = channelIds;
	}

	public void addChannelId(String id) {
		channelIds[channelIds.length-1] = id;
	}

	@Override
	public String toString() {
		return "User [idUser=" + idUser + ", name=" + name + ", pwd=" + pwd + ", photoId=" + photoId + ", channelIds="
				+ Arrays.toString(channelIds) + "]";
	}

}
