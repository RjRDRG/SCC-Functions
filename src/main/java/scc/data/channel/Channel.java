package scc.data.channel;

import java.util.Arrays;

public class Channel {
	private String id;
	private String name;
	private String owner;
	private boolean publicChannel;
	private String[] members;

	public Channel() {
	}

	public Channel(String id, String name, String owner, boolean publicChannel, String[] members) {
		super();
		this.id = id;
		this.members = members;
		this.name = name;
		this.owner = owner;
		this.publicChannel = publicChannel;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String[] getMembers() {
		return members;
	}

	public void setMembers(String[] members) {
		this.members = members;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public boolean isPublicChannel() {
		return publicChannel;
	}

	public void setPublicChannel(boolean publicChannel) {
		this.publicChannel = publicChannel;
	}

	@Override
	public String toString() {
		return "Channel{" +
				"id='" + id + '\'' +
				", name='" + name + '\'' +
				", owner='" + owner + '\'' +
				", publicChannel=" + publicChannel +
				", members=" + Arrays.toString(members) +
				'}';
	}
}
