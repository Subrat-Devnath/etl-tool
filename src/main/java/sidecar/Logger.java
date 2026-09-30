package sidecar;

public interface Logger {

    default org.apache.log4j.Logger log() {
        return org.apache.log4j.Logger.getLogger(getClass().getName());
    }

}
