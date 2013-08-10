package wikigraph;
/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 */
public class RevisionRecord implements Writable {
 /*
    <revision>
    <id>4407235</id>
    <parentid>2527990</parentid>
    <timestamp>2004-02-25T18:55:21Z</timestamp>
    <contributor>
      <username>Dori</username>
      <id>6878</id>
    </contributor>
    <minor/>
    <comment>restoring blanked content, no reason given</comment>
    <text id="4407235" bytes="538" />
    <sha1>cgc468uc4empeg5jggze1lsb87vquuc</sha1>
    <model>wikitext</model>
    <format>text/x-wiki</format>
  </revision>
*/
  private int namespace;
	private long time;
	private long timeToNextEdit;
	//private String username;
	private String article;
	private int length;
	private int bytesAdded;
	private int bytesRemoved;
	private String username;


  public RevisionRecord() {}
	public RevisionRecord(int ns, long time, long timeToNextEdit, String article, int l){
		namespace = ns;
		this.time = time;
		this.article = article;
		length = l;
	}

  
	 /* Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
	  	namespace = in.readInt();
		article = in.readUTF();
	  	length = in.readInt();
	  	//username = in.readUTF();
	  	time = in.readLong();
	  	timeToNextEdit = in.readLong();
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(namespace);
		out.writeUTF(article);
		out.writeInt(length);
		//out.writeUTF(username);
		out.writeLong(time);
		out.writeLong(timeToNextEdit);

	}

	@Override
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  //sb.append("Revision Record [");
	  sb.append("[");
	  sb.append(namespace + ",");
	  sb.append(article + ",");
	  sb.append(time + ",");
	  sb.append(timeToNextEdit + ",");
	  sb.append(length + "]");
	  return sb.toString();
    }
     
    
  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static RevisionRecord create(DataInput in) throws IOException {
    RevisionRecord m = new RevisionRecord();
    m.readFields(in);
    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static RevisionRecord create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }


public int getLength() {
	return length;
}


public void setLength(int length) {
	this.length = length;
}



public int getNamespace() {
	return namespace;
}



public void setNamespace(int namespace) {
	this.namespace = namespace;
}



public long getTime() {
	return time;
}



public void setTime(long time) {
	this.time = time;
}


public String getArticle() {
	return article;
}



public void setArticle(String article) {
	this.article = article;
}
public long getTimeToNextEdit() {
	return timeToNextEdit;
}
public void setTimeToNextEdit(long timeToNextEdit) {
	this.timeToNextEdit = timeToNextEdit;
}
public String getUsername() {
	return username;
}
public void setUsername(String username) {
	this.username = username;
}
public int getBytesAdded() {
	return bytesAdded;
}
public void setBytesAdded(int bytesAdded) {
	this.bytesAdded = bytesAdded;
}
public int getBytesRemoved() {
	return bytesRemoved;
}
public void setBytesRemoved(int bytesRemoved) {
	this.bytesRemoved = bytesRemoved;
}
}
