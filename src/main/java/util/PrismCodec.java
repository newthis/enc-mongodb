package util;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;

public class PrismCodec{



    // cotnent需要以utf-8编码
    //加密
    public String Encode(String content){
        try {

            String encodeRules = "prism.guard.2018" ;

            KeyGenerator keygen=KeyGenerator.getInstance("AES");

            keygen.init(128, new SecureRandom(encodeRules.getBytes()));

            SecretKey original_key=keygen.generateKey();

            byte [] raw=original_key.getEncoded();

            SecretKey key=new SecretKeySpec(raw, "AES");

            Cipher cipher=Cipher.getInstance("AES");

            cipher.init(Cipher.ENCRYPT_MODE, key);

            byte [] byte_encode=content.getBytes("utf-8");

            byte [] byte_AES=cipher.doFinal(byte_encode);

            String encodeCotent =new String(new BASE64Encoder().encode(byte_AES));

            return encodeCotent ;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        return "";
    }

    // cotnent需要以utf-8编码
    // 解密
    public String Decode(String content){
        try {
            String encodeRules = "prism.guard.2018" ;

            KeyGenerator keygen=KeyGenerator.getInstance("AES");

            keygen.init(128, new SecureRandom(encodeRules.getBytes()));

            SecretKey original_key=keygen.generateKey();

            byte [] raw=original_key.getEncoded();

            SecretKey key=new SecretKeySpec(raw, "AES");

            Cipher cipher=Cipher.getInstance("AES");

            cipher.init(Cipher.DECRYPT_MODE, key);

            byte [] byte_content= new BASE64Decoder().decodeBuffer(content);

            byte [] byte_decode=cipher.doFinal(byte_content);

            String decodeContent=new String(byte_decode,"utf-8");

            return decodeContent;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        }


        return "";
    }





    public static void main(String[] args) {
//        util.ConvertHelper se = new util.ConvertHelper();
//        PrismCodec cosc = new PrismCodec() ;
//        String previous = "434daga" ;
//        String hel = cosc.Encode(previous) ;
//        String hh = cosc.Decode(hel) ;
//        System.out.println(hh) ;
//        System.out.println(hel) ;
//        System.out.println(previous) ;

    }

}
