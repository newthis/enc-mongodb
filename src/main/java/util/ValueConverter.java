package util;

public class ValueConverter {
    public static long convertLong(long value){
        long dd = (long)(-value ^ 236 + 1265) ;
        return dd ;
    }

    public static long reconvertLong(long value){

        long real = (long)(-((value - 1265) ^ 236)) ;
        return real ;

    }
    public static int convertInteger(int value){
        int dd = -value ^ 56 + 65 ;
        return dd ;
    }

    public static int reconvertInteger(int value){

        int real = -((value - 65) ^ 56) ;
        return real ;

    }

    public static double convertDouble(double value){
        return (value + 12000.0) ;
//        byte [] byts = ByteHelper.doubleToBytes(value) ;
//        double ret = ByteHelper.bytesToDouble(ConvertHelper.AESEncodeBytes(byts)) ;
//
//        return ret ;
    }

    public static double reconvertDouble(double value){
        return (value - 12000.0) ;
//        byte [] byts = ByteHelper.doubleToBytes(value) ;
//        double ret = ByteHelper.bytesToDouble(ConvertHelper.AESDncodeBytes(byts)) ;
//        return ret ;


    }

    public static float convertFloat(float value){
        return (float)(value - 232.0) ;
//        byte [] byts = ByteHelper.floatToBytes(value) ;
//        float ret = ByteHelper.bytesToFloat(ConvertHelper.AESEncodeBytes(byts)) ;
//        return ret ;


    }
    public static float reconvertFloat(float value){
//        byte [] byts = ByteHelper.floatToBytes(value) ;
//        float ret = ByteHelper.bytesToFloat(ConvertHelper.AESDncodeBytes(byts)) ;
//        return ret ;
        return (float)(value + 232.0) ;



    }

    public static String convertString(String value){
        return ConvertHelper.AESEncode(value) ;

    }

    public static String reconvertString(String value){
        return ConvertHelper.AESDncode(value) ;

    }

    public static byte convertByte(byte value){
        return value ;

    }

    public static byte reconvertByte(byte value){
        return value ;

    }



    public static short convertShort(short value){
        byte [] byts = ByteHelper.shortToBytes(value) ;
        return ByteHelper.bytesToShort(ConvertHelper.AESEncodeBytes(byts)) ;


    }

    public static short reconvertShort(short value){
        byte [] byts = ByteHelper.shortToBytes(value) ;
        return ByteHelper.bytesToShort(ConvertHelper.AESDncodeBytes(byts)) ;


    }

    public static char convertChar(char value){
        return value ;

    }

    public static char reconvertChar(char value){
        return value ;
    }

    public static boolean convertBoolean(boolean value){
        if(value){
            return false ;

        }else{
            return true ;
        }

    }

    public static boolean reconvertBoolean(boolean value){
        if(value){
            return false ;

        }else{
            return true ;
        }

    }


}
