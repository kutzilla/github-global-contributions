package de.fhms.mdm.github.data.ingest.client;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by Dave on 20.02.2016.
 */
public class RepositoryWritable implements WritableComparable<RepositoryWritable>, Serializable {
    private static final long serialVersionUID = 1000L;

    private Text owner;
    private Text repo;
    private Text eTag;
    private Text lastModifiedSince;
    private Text committer;

    public RepositoryWritable(Text owner, Text repo, Text eTag, Text lastModifiedSince,Text committer) {
        this.owner = owner;
        this.repo = repo;
        this.eTag = eTag;
        this.lastModifiedSince = lastModifiedSince;
        this.committer = committer;
    }

    public  RepositoryWritable(){
        owner = new Text();
        repo = new Text();
        eTag = new Text();
        lastModifiedSince = new Text();
        committer = new Text();
    }

    public void write(DataOutput dataOutput) throws IOException {
        owner.write(dataOutput);
        repo.write(dataOutput);
        eTag.write(dataOutput);
        lastModifiedSince.write(dataOutput);
        committer.write(dataOutput);
    }

    private void clear(){
        this.owner = new Text();
        this.repo = new Text();
        this.eTag = new Text();
        this.lastModifiedSince = new Text();
        this.committer = new Text();
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.clear();
        owner.readFields(dataInput);
        repo.readFields(dataInput);
        eTag.readFields(dataInput);
        lastModifiedSince.readFields(dataInput);
        committer.readFields(dataInput);
    }

    public Text getOwner() {
        return owner;
    }

    public void setOwner(Text owner) {
        this.owner = owner;
    }

    public Text getRepo() {
        return repo;
    }

    public void setRepo(Text repo) {
        this.repo = repo;
    }

    public Text geteTag() {
        return eTag;
    }

    public void seteTag(Text eTag) {
        this.eTag = eTag;
    }

    public Text getLastModifiedSince() {
        return lastModifiedSince;
    }

    public void setLastModifiedSince(Text lastModifiedSince) {
        this.lastModifiedSince = lastModifiedSince;
    }

    public Text getCommitter() {
        return committer;
    }

    public void setCommitter(Text committer) {
        this.committer = committer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepositoryWritable that = (RepositoryWritable) o;

        if (owner != null ? !owner.equals(that.owner) : that.owner != null) return false;
        if (repo != null ? !repo.equals(that.repo) : that.repo != null) return false;
        if (eTag != null ? !eTag.equals(that.eTag) : that.eTag != null) return false;
        if (lastModifiedSince != null ? !lastModifiedSince.equals(that.lastModifiedSince) : that.lastModifiedSince != null)
            return false;
        return committer != null ? committer.equals(that.committer) : that.committer == null;

    }

    @Override
    public int hashCode() {
        int result = owner != null ? owner.hashCode() : 0;
        result = 31 * result + (repo != null ? repo.hashCode() : 0);
        result = 31 * result + (eTag != null ? eTag.hashCode() : 0);
        result = 31 * result + (lastModifiedSince != null ? lastModifiedSince.hashCode() : 0);
        result = 31 * result + (committer != null ? committer.hashCode() : 0);
        return result;
    }

    public int compareTo(RepositoryWritable o) {
        return ComparisonChain.start().compare(owner,o.owner).compare(repo, o.repo).compare(committer, o.committer).result();
    }
}
