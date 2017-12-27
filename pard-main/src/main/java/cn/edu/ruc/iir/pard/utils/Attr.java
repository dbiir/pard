package cn.edu.ruc.iir.pard.utils;

import java.io.Serializable;

public class Attr implements Serializable{
    AttrType type;//i, f, s
    int size;

    public Attr(AttrType type, int size) {
        this.type = type;
        this.size = size;
    }

    //for testing serialization
    @Override
    public String toString() {
        return "Attr{" +
                "type=" + type +
                ", size=" + size +
                '}';
    }
}
