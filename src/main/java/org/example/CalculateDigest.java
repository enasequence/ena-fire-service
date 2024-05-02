package org.example;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Provider.Service;
import java.security.Security;
import java.util.Set;


public class
CalculateDigest 
{
    static public String 
    calculateDigest( String digest_name, 
                     StringBuilder contents ) throws IOException, NoSuchAlgorithmException 
    {
        MessageDigest digest = MessageDigest.getInstance( digest_name );
        char[] buf = new char[ 4096 ];
        int start = 0;
        final int read  = buf.length;
        final int len = contents.length();
        do
        {
            
            contents.getChars( start, 
                               ( start + read ) > len ? len : start + read, 
                               buf, 
                               0 );
            digest.update( new String( buf, 0, ( start + read ) > len ? len - start : read ).getBytes() );
        }
        while( ( start += buf.length ) < contents.length() );
            
        byte[] message_digest = digest.digest();
        BigInteger value = new BigInteger( 1, message_digest );
        return String.format( String.format( "%%0%dx", message_digest.length << 1 ), value );
    }
    
    
    static public String 
    calculateDigest( String digest_name, 
                     File   file ) throws IOException, NoSuchAlgorithmException 
    {
        MessageDigest digest = MessageDigest.getInstance( digest_name );
        byte[] buf = new byte[ 4096 ];
        int  read  = 0;
        try( BufferedInputStream is = new BufferedInputStream( new FileInputStream( file ) ) )
        {
            while( ( read = is.read( buf ) ) > 0 )
                digest.update( buf, 0, read );
            
            byte[] message_digest = digest.digest();
            BigInteger value = new BigInteger( 1, message_digest );
            return String.format( String.format( "%%0%dx", message_digest.length << 1 ), value );
        }
    }
    
    public static String
    listAvailableDigests()
    {
        Provider[] providers = Security.getProviders();
        StringBuilder sb = new StringBuilder();
        for (Provider p : providers) 
        {
          Set<Service> services = p.getServices();
          for( Service s : services ) 
          {
            if( "MessageDigest".equals( s.getType() ) )
               sb.append( s.getAlgorithm() )
                 .append( ',' );
          }
        }
        return sb.substring( 0, sb.length() - 1 );
    }
}
