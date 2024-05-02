package org.example;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.Base64;

public class
HttpHeaderBuilder
{
    private final HttpHeaders headers = new HttpHeaders();

    public HttpHeaderBuilder
    set( String headerName, Object headerValue )
    {
        headers.set( headerName, String.valueOf( headerValue ) );
        return this;
    }


    public HttpHeaderBuilder
    basicAuth( String userName, String password )
    {
        return set( "Authorization", basicAuthValue(userName, password ));
    }

    public static String
    basicAuthValue( String userName, String password )
    {
        String auth = userName + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString( auth.getBytes());
    }

    public HttpHeaderBuilder
    multipartFormData( long contentLength )
    {
        headers.setContentType( MediaType.MULTIPART_FORM_DATA );
        headers.setContentLength( contentLength );
        return this;
    }


    public HttpHeaderBuilder
    multipartFormData()
    {
        headers.setContentType( MediaType.MULTIPART_FORM_DATA );
        return this;
    }


    public HttpHeaderBuilder
    applicationJSON()
    {
        headers.setContentType( MediaType.APPLICATION_JSON );
        return this;
    }


    public HttpHeaderBuilder
    contentType( MediaType type )
    {
        headers.setContentType( type );
        return this;
    }


    public HttpHeaders
    build()
    {
        return headers;
    }
}