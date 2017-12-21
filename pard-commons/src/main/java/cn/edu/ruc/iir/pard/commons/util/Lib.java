// PART OF THE MACHINE SIMULATION. DO NOT CHANGE.

package cn.edu.ruc.iir.pard.commons.util;

import java.util.Random;

/**
 * <p>
 * 提供多种多多样的库例程
 * </p>
 * <p>
 * Provides miscellaneous library routines.
 * </p>
 * 个人感觉负责六件事：
 * <ul>
 * <li>判断表达式的值是否为真，如果是假，则Machine终止
 * <li>判断某个类是否存在，以及方法名、参数列表、返回值类型、标识符是否符合规范
 * <li>通过反射机制将nachos.conf中的一些类名构造成对应的类
 * <li>随机数工具，这些工具可以模拟机器时间的不稳定性{@link Timer#scheduleInterrupt}
 * <li>debug和数值转换工具
 * <li>提供文件读取服务
 * </ul>
 */
public final class Lib
{
    /**
     * Prevent instantiation. 防止初始化
     */
    private Lib()
    {
    }

    private static Random random = null;

    /**
     * Seed the random number generater. May only be called once.
     * 生成随机数的种子，只应该被调用一次
     * @param randomSeed
     *            the seed for the random number generator.
     */
    public static void seedRandom(long randomSeed)
    {
        random = new Random(randomSeed);
    }
    /**
     * Return a random double between 0.0 (inclusive) and 1.0 (exclusive).
     * 返回0.0（包含）到1.0（不包含）之间的随机数
     * @return a random double between 0.0 and 1.0.
     */
    public static double random()
    {
        return random.nextDouble();
    }
    /**
     * Convert a short into its little-endian byte string representation.
     * 将short转byte[]
     * @param array
     *            the array in which to store the byte string.
     * @param offset
     *            the offset in the array where the string will start.
     * @param value
     *            the value to convert.
     */
    public static void bytesFromShort(byte[] array, int offset, short value)
    {
        array[offset + 0] = (byte) ((value >> 0) & 0xFF);
        array[offset + 1] = (byte) ((value >> 8) & 0xFF);
    }

    /**
     * Convert an int into its little-endian byte string representation.
     * int转byte[]
     *
     * @param array
     *            the array in which to store the byte string.
     * @param offset
     *            the offset in the array where the string will start.
     * @param value
     *            the value to convert.
     */
    public static void bytesFromInt(byte[] array, int offset, int value)
    {
        array[offset + 0] = (byte) ((value >> 0) & 0xFF);
        array[offset + 1] = (byte) ((value >> 8) & 0xFF);
        array[offset + 2] = (byte) ((value >> 16) & 0xFF);
        array[offset + 3] = (byte) ((value >> 24) & 0xFF);
    }

    /**
     * Convert an int into its little-endian byte string representation, and
     * return an array containing it. int转byte[]
     * @param value
     *            the value to convert.
     * @return an array containing the byte string.
     */
    public static byte[] bytesFromInt(int value)
    {
        byte[] array = new byte[4];
        bytesFromInt(array, 0, value);
        return array;
    }

    /**
     * Convert an int into a little-endian byte string representation of the
     * specified length. 将int转成特定字节大小的byte[]
     *
     * @param array
     *            the array in which to store the byte string.
     * @param offset
     *            the offset in the array where the string will start.
     * @param length
     *            the number of bytes to store (must be 1, 2, or 4).
     * @param value
     *            the value to convert.
     */
    public static void bytesFromInt(byte[] array, int offset, int length, int value)
    {
        switch (length) {
            case 1:
                array[offset] = (byte) value;
                break;
            case 2:
                bytesFromShort(array, offset, (short) value);
                break;
            case 4:
                bytesFromInt(array, offset, value);
                break;
        }
    }

    /**
     * Convert to a short from its little-endian byte string representation.
     * 将byte转成特定字节大小的short
     * @param array
     *            the array containing the byte string.
     * @param offset
     *            the offset of the byte string in the array.
     * @return the corresponding short value.
     */
    public static short bytesToShort(byte[] array, int offset)
    {
        return (short) ((((short) array[offset + 0] & 0xFF) << 0) | (((short) array[offset + 1] & 0xFF) << 8));
    }

    /**
     * Convert to an unsigned short from its little-endian byte string
     * representation. 将byte转成特定字节大小的unsigned Short
     * @param array
     *            the array containing the byte string.
     * @param offset
     *            the offset of the byte string in the array.
     * @return the corresponding short value.
     */
    public static int bytesToUnsignedShort(byte[] array, int offset)
    {
        return (((int) bytesToShort(array, offset)) & 0xFFFF);
    }

    /**
     * Convert to an int from its little-endian byte string representation.
     * byte[]转int
     *
     * @param array
     *            the array containing the byte string.
     * @param offset
     *            the offset of the byte string in the array.
     * @return the corresponding int value.
     */
    public static int bytesToInt(byte[] array, int offset)
    {
        return (int) ((((int) array[offset + 0] & 0xFF) << 0) | (((int) array[offset + 1] & 0xFF) << 8)
                | (((int) array[offset + 2] & 0xFF) << 16) | (((int) array[offset + 3] & 0xFF) << 24));
    }

    /**
     * Convert to an int from a little-endian byte string representation of the
     * specified length. byte转特定长度的int
     * @param array
     *            the array containing the byte string.
     * @param offset
     *            the offset of the byte string in the array.
     * @param length
     *            the length of the byte string.
     * @return the corresponding value.
     */
    public static int bytesToInt(byte[] array, int offset, int length)
    {
        switch (length) {
            case 1:
                return array[offset];
            case 2:
                return bytesToShort(array, offset);
            case 4:
                return bytesToInt(array, offset);
            default:
                return -1;
        }
    }

    /**
     * Convert to a string from a possibly null-terminated array of bytes.
     * 将一个bytes转化成String,byte从offset开头到空字符或array.length结尾
     * @param array
     *            the array containing the byte string.
     * @param offset
     *            the offset of the byte string in the array.
     * @param length
     *            the maximum length of the byte string.
     * @return a string containing the specified bytes, up to and not including
     *         the null-terminator (if present).
     */
    public static String bytesToString(byte[] array, int offset, int length)
    {
        int i;
        for (i = 0; i < length; i++) {
            if (array[offset + i] == 0) {
                break;
            }
        }

        return new String(array, offset, i);
    }

    /**
     * Mask out and shift a bit substring. 将低位遮住然后右移，简称：‘高位提取’
     * @param bits
     *            the bit string.
     * @param lowest
     *            the first bit of the substring within the string.
     * @param size
     *            the number of bits in the substring.
     * @return the substring.
     */
    public static int extract(int bits, int lowest, int size)
    {
        if (size == 32) {
            return (bits >> lowest);
        }
        else {
            return ((bits >> lowest) & ((1 << size) - 1));
        }
    }

    /**
     * Mask out and shift a bit substring. 将低位遮住然后右移，简称：‘高位提取’
     *
     * @param bits
     *            the bit string.
     * @param lowest
     *            the first bit of the substring within the string.
     * @param size
     *            the number of bits in the substring.
     * @return the substring.
     */
    public static long extract(long bits, int lowest, int size)
    {
        if (size == 64) {
            return (bits >> lowest);
        }
        else {
            return ((bits >> lowest) & ((1L << size) - 1));
        }
    }

    /**
     * Mask out and shift a bit substring; then sign extend the substring.
     * @param bits
     *            the bit string.
     * @param lowest
     *            the first bit of the substring within the string.
     * @param size
     *            the number of bits in the substring.
     * @return the substring, sign-extended.
     */
    public static int extend(int bits, int lowest, int size)
    {
        int extra = 32 - (lowest + size);
        return ((extract(bits, lowest, size) << extra) >> extra);
    }
    /**
     * Creates a padded upper-case string representation of the integer argument
     * in base 16. 创建一个代表16位整数且字母大写的String，该String为至少8位
     * @param i
     *            an integer.
     * @return a padded upper-case string representation in base 16.
     */
    public static String toHexString(int i)
    {
        return toHexString(i, 8);
    }

    /**
     * Creates a padded upper-case string representation of the integer argument
     * in base 16, padding to at most the specified number of digits.
     * 创建一个代表16位整数且字母大写的String，如果不足pad位高位补0
     * @param i
     *            an integer.
     * @param pad
     *            the minimum number of hex digits to pad to.
     * @return a padded upper-case string representation in base 16.
     */
    public static String toHexString(int i, int pad)
    {
        String result = Integer.toHexString(i).toUpperCase();
        while (result.length() < pad) {
            result = "0" + result;
        }
        return result;
    }
}
