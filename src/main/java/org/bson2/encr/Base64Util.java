package org.bson2.encr;


import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

public class Base64Util {





    public static String encodeString(byte [] bytes){

        final BASE64Encoder encoder = new BASE64Encoder();

        final String encodedText = encoder.encode(bytes);
        return encodedText ;



    }


    public static  byte [] decodeString(String sdf){

        final BASE64Decoder decoder = new BASE64Decoder() ;
        try{


            return decoder.decodeBuffer(sdf) ;
        }catch(java.io.IOException e){

            //e.printStackTrace();
            return new byte[0] ;


        }


    }




}
