package org.example;

import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

public class FireS3File {
    private String fireOid;
    private String md5;
    private Long size;
    private String projection;

    public FireS3File(String fireOid, String md5, Long size, String projection) {
        this.fireOid = fireOid;
        this.md5 = md5;
        this.size = size;
        this.projection = projection;
    }

    public String getFireOid() {
        return fireOid;
    }

    public String getMd5() {
        return md5;
    }

    public Long getSize() {
        return size;
    }

    public String getProjection() {
        return projection;
    }


    public static FireS3File fromHeadObjectResponse(HeadObjectResponse response, String s3FilePath) {
        String fireOid = response.metadata().getOrDefault("Fireoid", "");
        String md5 = response.eTag().replace("\"", ""); // Remove quotes from ETag for MD5
        Long fileSize = response.contentLength(); // Get file size
        return new FireS3File(fireOid, md5, fileSize, s3FilePath);
    }
}
