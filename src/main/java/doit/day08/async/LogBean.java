package doit.day08.async;

public class LogBean {

    public String uid;

    public Double longitude;

    public Double latitude;

    public String province;

    public String city;

    public LogBean(){}

    public LogBean(String uid, Double longitude, Double latitude) {
        this.uid = uid;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public static LogBean of(String uid, Double longitude, Double latitude) {
        return new LogBean(uid, longitude, latitude);
    }

    @Override
    public String toString() {
        return "LogBean{" +
                "uid='" + uid + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
