package de.fhms.mdm.github_data_processing;

import java.util.Objects;

/**
 * Created by Matthias on 20.02.16.
 */
public class Commit {

    private String message;

    private String commitDate;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getCommitDate() {
        return commitDate;
    }

    public void setCommitDate(String commitDate) {
        this.commitDate = commitDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Commit commit = (Commit) o;
        return Objects.equals(message, commit.message) &&
                Objects.equals(commitDate, commit.commitDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, commitDate);
    }

    @Override
    public String toString() {
        return "Commit{" +
                "message='" + message + '\'' +
                ", commitDate='" + commitDate + '\'' +
                '}';
    }
}
