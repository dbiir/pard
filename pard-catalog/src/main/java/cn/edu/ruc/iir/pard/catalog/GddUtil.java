package cn.edu.ruc.iir.pard.catalog;

import java.util.HashMap;
import java.util.Map;

public class GddUtil
{
    public static final int privilegeREADONLY = 1;
    public static final int privilegeWRITE = 3;
    public static final int privilegeOWNER = 5;
    public static final int fragementHORIZONTIAL = 1;
    public static final int fragmentVERTICAL = 2;
    public static final int datatypeINT = 1;
    public static final int datatypeNUMBER = 2;
    public static final int datatypeCHAR = 3;
    public static final int datatypeVARCHAR = 4;
    public static final int datatypeDATE = 5;
    public static final int compareEQUAL = 1;
    public static final int compareLESS = 2;
    public static final int compareLESSEQUAL = 3;
    public static final int compareGREAT = 4;
    public static final int compareGREATEQUAL = 5;
    public static final int compareNOTEQUAL = 6;
    public static final int indexHASHINDEX = 1;
    public static final int indexBTREEINDEX = 2;
    public static final int indexOTHERINDEX = 3;

    private static final Map<Integer, String> cmpMap = new HashMap<Integer, String>();

    private GddUtil()
    {
    }
    private static void initCmpMap()
    {
        cmpMap.put(compareEQUAL, "=");
        cmpMap.put(compareGREAT, ">");
        cmpMap.put(compareGREATEQUAL, ">=");
        cmpMap.put(compareLESS, "<");
        cmpMap.put(compareLESSEQUAL, "<=");
        cmpMap.put(compareNOTEQUAL, "!=");
    }
    public static String cmpInt2Str(int compare)
    {
        if (cmpMap.size() <= 0) {
            initCmpMap();
        }
        return cmpMap.get(compare);
    }
}
