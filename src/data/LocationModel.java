package data;

import java.math.BigDecimal;

public class LocationModel {

  protected Long id;
  protected String name;
  protected String type;
  protected String address;
  protected String district;
  protected String city;
  protected String postcode;
  protected String country;
  protected String elevation;
  protected String population;
  protected BigDecimal latitude;
  protected BigDecimal longitude;
  private String website;
  private String description;
  private String email;
  private String phone;
  protected String date;
  protected String schedule;
  protected String urlImage;
  protected String csvName;
  protected String other;
  protected double rating = 0;
  private int nrating = 0;

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public String getElevation() {
    return elevation;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public void setElevation(String elevation) {
    this.elevation = elevation;
  }

  public String getPopulation() {
    return population;
  }

  public void setPopulation(String population) {
    this.population = population;
  }

  public String getUrlImage() {
    return urlImage;
  }

  public void setUrlImage(String urlImage) {
    this.urlImage = urlImage;
  }

  public LocationModel() {}

  public String getAddress() {
    return address;
  }

  public String getCsvName() {
    return csvName;
  }

  public String getDate() {
    return date;
  }

  public String getDescription() {
    return description;
  }

  public String getDistrict() {
    return district;
  }

  public String getPostcode() {
    return postcode;
  }

  public String getCity() {
    return city;
  }

  public String getEmail() {
    return email;
  }

  public Long getId() {
    return id;
  }

  public String getImage() {
    return urlImage;
  }

  public BigDecimal getLatitude() {
    return latitude;
  }

  public BigDecimal getLongitude() {
    return longitude;
  }

  public String getName() {
    return name;
  }

  public int getNrating() {
    return nrating;
  }

  public String getPhone() {
    return phone;
  }

  public double getRating() {
    return rating;
  }

  public String getSchedule() {
    return schedule;
  }

  public String getType() {
    return type;
  }

  public String getWebsite() {
    return website;
  }

  public String getOther() {
    return other;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public void setCsvName(String csvName) {
    this.csvName = csvName;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setDistrict(String district) {
    this.district = district;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public void setPostcode(String postcode) {
    this.postcode = postcode;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public void setImage(String urlImage) {
    this.urlImage = urlImage;
  }

  public void setLatitude(BigDecimal latitude) {
    this.latitude = latitude;
  }

  public void setLongitude(BigDecimal longitude) {
    this.longitude = longitude;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setNrating(int nrating) {
    this.nrating = nrating;
  }

  public void setPhone(String phone) {
    this.phone = phone;
  }

  public void setRating(double rating) {
    this.rating = rating;
  }

  public void setSchedule(String schedule) {
    this.schedule = schedule;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setWebsite(String website) {
    this.website = website;
  }

  public void setOther(String other) {
    this.other = other;
  }
}
