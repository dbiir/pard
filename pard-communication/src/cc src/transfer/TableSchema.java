package transfer;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * abstraction  of Transferred Data
 */
public class TableSchema implements Serializable {
    Attr[] attrs;
    String[] attrNames;
    int nRecs;
    int linesize = 0;
    int tablesize = 0;


    public TableSchema(Attr[] attrs, String[] attrNames, int nRecs) {
        this.attrs = attrs;
        this.attrNames = attrNames;
        this.nRecs = nRecs;
        for (Attr a :
                attrs) {
            linesize += a.size;
        }
        tablesize = linesize * nRecs;

    }

    public Attr[] getAttrs() {
        return attrs;
    }

    public int getAttrNum() {
        return attrs.length;
    }

    public int getnRecs() {
        return nRecs;
    }

    public int getTablesize() {
        return tablesize;
    }

    public int getLinesize() {
        return linesize;
    }

    public String[] getAttrNames() {
        return attrNames;
    }

    public byte[] serialize() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(this);
        return out.toByteArray();
    }



    @Override
    public String toString() {
        return "TableSchema{" +
                "attrs=" + Arrays.toString(attrs) +
                ", attrNames=" + Arrays.toString(attrNames) +
                ", nRecs=" + nRecs +
                ", linesize=" + linesize +
                ", tablesize=" + tablesize +
                '}';
    }
}
