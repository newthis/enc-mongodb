package util;

import java.nio.ByteBuffer;

public class ByteHelper {
    public static short bytesToShort(byte[] bytes){
        if(bytes == null){
            return 0 ;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES);
        byteBuffer.put(bytes);
        byteBuffer.flip();
        return byteBuffer.getShort();
    }


    public static byte [] shortToBytes(short number) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES);
        byteBuffer.putShort(number);
        return byteBuffer.array();
    }

    public static float bytesToFloat(byte[] floatBytes){
        if(floatBytes == null){
            return 0 ;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(Float.BYTES);
        byteBuffer.put(floatBytes);
        byteBuffer.flip();
        return byteBuffer.getFloat();
    }


    public static byte [] floatToBytes(float number) {

        ByteBuffer byteBuffer = ByteBuffer.allocate(Float.BYTES);
        byteBuffer.putFloat(number);
        return byteBuffer.array();
    }

    public static double bytesToDouble(byte[] doubleBytes){
        if(doubleBytes == null){
            return 0 ;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
        byteBuffer.put(doubleBytes);
        byteBuffer.flip();
        return byteBuffer.getDouble();
    }


    public static byte [] doubleToBytes(double number) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
        byteBuffer.putDouble(number);
        return byteBuffer.array();
    }

    public static byte[] int2byte(int res) {
        byte[] targets = new byte[4];

        targets[3] = (byte) (res & 0xff);// 最低位
        targets[2] = (byte) ((res >> 8) & 0xff);// 次低位
        targets[1] = (byte) ((res >> 16) & 0xff);// 次高位
        targets[0] = (byte) (res >>> 24);// 最高位,无符号右移。
        return targets;
    }

    public static int byteArrayToInt(byte[] b){
        if(b == null){
            return 0 ;
        }
        byte[] a = new byte[4];
        int i = a.length - 1,j = b.length - 1;
        for (; i >= 0 ; i--,j--) {//从b的尾部(即int值的低位)开始copy数据
            if(j >= 0)
                a[i] = b[j];
            else
                a[i] = 0;//如果b.length不足4,则将高位补0
        }
        int v0 = (a[0] & 0xff) << 24;//&0xff将byte值无差异转成int,避免Java自动类型提升后,会保留高位的符号位
        int v1 = (a[1] & 0xff) << 16;
        int v2 = (a[2] & 0xff) << 8;
        int v3 = (a[3] & 0xff) ;
        return v0 + v1 + v2 + v3;
    }

    public static byte[] long2byte(long res) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((res >> offset) & 0xff);
        }
        return buffer;
    }

    public static long byteArrayToLong(byte[] b){
        if( b == null){
            return 0L ;
        }
        long values = 0;
        for (int i = 0; i < 8; i++) {
            values <<= 8; values|= (b[i] & 0xff);
        }
        return values;
    }

    public static void main(String [] args){
//        double number = 2.343 ;
//        System.out.println(ByteHelper.convertByteArrayToDouble(ByteHelper.convertDoubleToByteArray(number))) ;

    }
}
