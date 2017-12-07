package transfer;

import java.io.*;

public class SerializationUtils {

    public static byte[] serialize(Object obj) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("SerializationUtils.serialize ERROR");
        return null;
    }

    public static Object deserialize(byte[] data) {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = null;
        try {
            is = new ObjectInputStream(in);
            return is.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("SerializationUtils.deserialize ERROR");
        return null;
    }
}
