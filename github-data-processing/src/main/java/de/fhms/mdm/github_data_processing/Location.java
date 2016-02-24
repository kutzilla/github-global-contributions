package de.fhms.mdm.github_data_processing;

import java.io.Serializable;
import java.util.Objects;

/**
 * Created by Matthias on 23.02.16.
 */
public class Location implements Serializable {

    private static final long serialVersionUID = 10000L;

    private String city;

    private String longitude;

    private String latitude;

    private String country;

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Location location = (Location) o;
        return Objects.equals(city, location.city) &&
                Objects.equals(longitude, location.longitude) &&
                Objects.equals(latitude, location.latitude) &&
                Objects.equals(country, location.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(city, longitude, latitude, country);
    }

    @Override
    public String toString() {
        return "Location{" +
                "city='" + city + '\'' +
                ", longitude='" + longitude + '\'' +
                ", latitude='" + latitude + '\'' +
                ", country='" + country + '\'' +
                '}';
    }
}
