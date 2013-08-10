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
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;


/**
 * 
 * 
 */
public class UserProfile implements Writable {
	//Number of edits and number of article edits. Hypotheses about how these relate to bot/spam behavior.
	//Time from last edit and time to next edit.	
	
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

	private long nedits;
	private long narticles;
	private float meanTimeToNextEdit;
	private float meanEditBytes;

	private TreeMap<Long,Long> dayedits;
	private TreeMap<Long,Long> dayarticles;
	private TreeMap<Integer,Long> namespacecounts;
	
  public UserProfile() {}

  public UserProfile(long nedits, long narticles, float meanTime, float meanBytes){
		this.nedits = nedits;
		this.narticles = narticles;
		this.meanTimeToNextEdit = meanTime;
		this.meanEditBytes = meanBytes;
		//dayedits = new TreeMap<Long,Long>();
		//dayarticles = new TreeMap<Long,Long>();
	}

  
	 /* Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
	  	nedits = in.readLong();
		narticles = in.readLong();
		meanTimeToNextEdit = in.readFloat();
		meanEditBytes = in.readFloat();
		
		dayedits = new TreeMap<Long,Long>();
		dayarticles = new TreeMap<Long,Long>();
	  	namespacecounts = new TreeMap<Integer,Long>();

	  	int i=0;
	  	long key;
	  	long val;
	  	int nkeys = in.readInt();
	  	while(i<nkeys){
	  		key = in.readLong();
	  		val = in.readLong();
	  		dayedits.put(key, val);
	  		i++;
	  	}
	  	i=0;
	  	nkeys = in.readInt();
	  	while(i<nkeys){
	  		key = in.readLong();
	  		val = in.readLong();
	  		dayarticles.put(key, val);
	  		i++;
	  	}
	  	i=0;
	  	nkeys = in.readInt();
	  	int key2;
	  	while(i<nkeys){
	  		key2 = in.readInt();
	  		val = in.readLong();
	  		namespacecounts.put(key2, val);
	  		i++;
	  	}

	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(nedits);
		out.writeLong(narticles);
		out.writeFloat(meanTimeToNextEdit);
		out.writeFloat(meanEditBytes);
		
		out.writeInt(dayedits.keySet().size());
		for(long key : dayedits.keySet()){
			out.writeLong(key);
			out.writeLong(dayedits.get(key));
		}
		out.writeInt(dayarticles.keySet().size());
		for(long key : dayarticles.keySet()){
			out.writeLong(key);
			out.writeLong(dayarticles.get(key));
		}
		out.writeInt(namespacecounts.keySet().size());
		for(int key : namespacecounts.keySet()){
			out.writeInt(key);
			out.writeLong(namespacecounts.get(key));
		}

	}

	@Override
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  sb.append("[");
	  sb.append(nedits + ",");
	  sb.append(narticles + ",");
	  sb.append("[");
	  for(long key : dayedits.keySet()){
		 if(key != dayarticles.firstKey()) sb.append(",");
	  	 sb.append("{" + key + "," + dayedits.get(key) + "}");
	  }
	  sb.append("],");
	  sb.append("[");
	  for(long key : dayarticles.keySet()){
		if(key != dayarticles.firstKey()) sb.append(",");
	  	sb.append("{" + key + "," + dayarticles.get(key) + "}");
	  }
	  sb.append("],");
	  sb.append("[");
	  for(int key : namespacecounts.keySet()){
		if(key != namespacecounts.firstKey()) sb.append(",");
	  	sb.append("{" + key + "," + namespacecounts.get(key) + "}");
	  }
	  sb.append("],");
	  long activespan = dayedits.lastKey() - dayedits.firstKey();
	  sb.append(activespan);
	  sb.append(",");
	  sb.append(meanTimeToNextEdit);
	  sb.append(",");
	  sb.append(meanEditBytes);
	  sb.append("]");
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
  public static UserProfile create(DataInput in) throws IOException {
    UserProfile m = new UserProfile();
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
  public static UserProfile create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }


public long getNEdits() {
	return nedits;
}
public void setNEdits(long nedits) {
	this.nedits = nedits;
}
public long getNArticles() {
	return narticles;
}
public void setNArticles(long narticles) {
	this.narticles = narticles;
}

public void setEditMap(TreeMap<Long, Long> editmap){
	this.dayedits = editmap;
}

public void setArticleMap(TreeMap<Long, Long> articlemap){
	this.dayarticles = articlemap;
}

public TreeMap<Long,Long> getEditMap(){
	return dayedits;
	
}

public TreeMap<Long,Long> getArticleMap(){
	return dayarticles;
	
}

public void setNamespaceMap(TreeMap<Integer, Long> nscounts) {
	this.namespacecounts = nscounts;
	
}

public TreeMap<Integer,Long> getNamespaceMap(){
	return namespacecounts;
}

public float getMeanTimeToNextEdit() {
	return meanTimeToNextEdit;
}

public void setMeanTimeToNextEdit(float meanTimeToNextEdit) {
	this.meanTimeToNextEdit = meanTimeToNextEdit;
}


public float getMeanEditBytes() {
	return meanEditBytes;
}

public void setMeanEditBytes(float meanEditBytes) {
	this.meanEditBytes = meanEditBytes;
}
}