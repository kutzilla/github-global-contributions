package de.fhms.mdm.github.data.ingest.rest.service;

/**
 * Created by Dave on 18.02.2016.
 * Repo Id Object, kapselt owner und repo name
 */
public class GithubRepoId {

    private String owner;
    private String name;

    public GithubRepoId(String owner, String name){

        if(owner == null){
            throw new IllegalArgumentException("owner may not be null");
        }else if(name == null){
            throw new IllegalArgumentException("name may not be null");
        }else{
            this.owner = owner;
            this.name = name;
        }
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "GithubRepoId{" +
                "owner='" + owner + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
