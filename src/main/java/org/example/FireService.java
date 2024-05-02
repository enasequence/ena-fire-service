package org.example;

import org.apache.commons.codec.binary.Hex;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.http.HttpStatus.Series.CLIENT_ERROR;
import static org.springframework.http.HttpStatus.Series.SERVER_ERROR;

public class FireService extends AbstractService {
    public static final Region SIGNING_REGION = Region.US_EAST_1;
    final private String API_VERSION = "v1.1";
    final private String serviceUrl;
    final private String username;
    final private String password;
    final private String usernameS3;
    final private String tokenS3;
    final private String s3BucketName;

    public FireService(String service_url,
                       String username,
                       String password,
                       String usernameS3,
                       String tokenS3,
                       String s3BucketName) {
        this.serviceUrl = service_url;
        this.username = username;
        this.password = password;
        this.usernameS3 = usernameS3;
        this.tokenS3 = tokenS3;
        this.s3BucketName = s3BucketName;
    }

    public FireS3File
    uploadFileGetFire3FileObject(Path file, String projection) throws IOException, NoSuchAlgorithmException {
        uploadFile(file, projection);
        return queryPath(projection);
    }

    public FireS3File
    uploadFileGetFire3FileObject(Path file, String fileMd5, String projection) {
        uploadFile(file, fileMd5, projection);
        return queryPath(projection);
    }

    public String
    uploadFile(Path file, String projection) throws IOException, NoSuchAlgorithmException {
        String fileMd5 = CalculateDigest.calculateDigest("MD5", file.toFile());
        return uploadFile(file, fileMd5, projection);
    }

    public String
    uploadFile(Path file, String fileMd5, String projection) {
        return retryTemplate.execute(context -> uploadFileInternal(file, fileMd5, projection));
    }

    private String
    uploadFileInternal(Path file, String fileMd5, String projection) {
        try (S3Client s3Client = createS3Client()) {
            String encodedMd5 = Base64.getEncoder().encodeToString(Hex.decodeHex(fileMd5));
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .contentMD5(encodedMd5).build();

            PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest, file);
            String resultMd5 = putObjectResponse.eTag().replace("\"", "");

            if (resultMd5.equals(fileMd5)) {
                return resultMd5;
            } else {
                throw new DataFileException.UploadMd5Mismatch(
                        "Provided MD5 " + fileMd5 + " doesn't match S3 upload response MD5 " + resultMd5);
            }
        } catch (S3Exception e) {
            if (e.statusCode() == HttpStatus.CONFLICT.value()) {
                throw new DataFileException.AlreadyExists(projection + " file already exists");

            } else if (e.getMessage().contains("Content-MD5")
                        || e.getMessage().contains("The MD5 you specified did not match what we received")) {
                throw new DataFileException.Md5Mismatch(String.format(
                        FireResult.MD5_MISMATCH.reasonPrefix + "[%s]", e.getMessage()));
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String
    uploadFileMultipart(Path file, String projection) throws IOException, NoSuchAlgorithmException {
        String fileMd5 = CalculateDigest.calculateDigest("MD5", file.toFile());
        return uploadFileMultipart(file, fileMd5, projection);
    }

    public String
    uploadFileMultipart(Path file, String fileMd5, String projection) {
        return retryTemplate.execute(context -> uploadFileMultipartInternal(file, fileMd5, projection));
    }

    public String
    uploadFileMultipartInternal(Path file, String fileMd5, String projection) {
        try (
                S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
                        .endpointOverride(URI.create(serviceUrl))
                        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                        .credentialsProvider(StaticCredentialsProvider.
                                create(AwsBasicCredentials.create(usernameS3, tokenS3)))
                        .overrideConfiguration(
                                ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
                        .region(SIGNING_REGION).build();
                S3TransferManager transferManager = S3TransferManager.builder()
                        .s3Client(s3AsyncClient)
                        .build();
        ) {
            String encodedMd5 = Base64.getEncoder().encodeToString(Hex.decodeHex(fileMd5));
            UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                    .putObjectRequest(builder -> builder.bucket(s3BucketName).key(projection).contentMD5(encodedMd5))
                    .source(file)
                    .build();

            CompletedFileUpload completedUpload =
                    transferManager.uploadFile(uploadFileRequest).completionFuture().join();

            return completedUpload.response().eTag().replace("\"", "");
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload file to S3", e);
        }
    }

    public boolean
    deleteFile(String fireOid) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setOutputStreaming(false);
        requestFactory.setConnectTimeout(0);
        requestFactory.setReadTimeout(0);
        requestFactory.setBufferRequestBody(false);
        requestFactory.setOutputStreaming(false);

        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(requestFactory);
        restTemplate.setErrorHandler(new ErrorHandler());

        ResponseEntity<Object> response = restTemplate.exchange(
                serviceUrl + "/fire/{api_version}/objects/{fireOid}",
                HttpMethod.DELETE,
                new HttpEntity<>((new HttpHeaderBuilder()).basicAuth(username, password)
                        .build()),
                Object.class,
                API_VERSION,
                fireOid);

        return 204 == response.getStatusCodeValue();
    }

    public FireS3File
    queryPath(String projection) {
        return retryTemplate.execute(context -> queryPathInternal(projection));
    }

    private FireS3File
    queryPathInternal(String projection) {
        try (S3Client s3client = createS3Client()) {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .build();
            try {
                HeadObjectResponse headObjectResponse = s3client.headObject(headObjectRequest);
                return FireS3File.fromHeadObjectResponse(headObjectResponse, projection);
            } catch (NoSuchKeyException e) {
                return null; // Return null if the specified object does not exist
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Path
    downloadFile(String projection) throws IOException {
        return downloadFile(projection, Files.createTempFile("DOWNLOADED", "FROM_FIRE"));
    }

    public Path
    downloadFile(String projection, Path output) {
        return retryTemplate.execute(context -> downloadFileInternal(projection, output));
    }

    private Path downloadFileInternal(String projection, Path output) {
        if (null == usernameS3 || null == tokenS3 || null == s3BucketName) {
            throw new DataFileException.DownloadFailure("S3 credentials are not set");
        }

        try (S3Client s3Client = createS3Client()) {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .build();

            ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(getObjectRequest);

            Files.copy(inputStream, output, StandardCopyOption.REPLACE_EXISTING);
            inputStream.close();

            return output;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String moveFile(String oldProjection, String newProjection) {
        return retryTemplate.execute(context -> moveFileInternal(oldProjection, newProjection));
    }

    private String moveFileInternal(String oldProjection, String newProjection) {
        try (S3Client s3Client = createS3Client()) {
            // Prepare the metadata directive for the move operation, if required
            // Note: Adjust this according to your backend's needs
            CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                    .destinationBucket(s3BucketName)
                    .destinationKey(newProjection)
                    .sourceBucket(s3BucketName)
                    .sourceKey(oldProjection)
                    .metadataDirective(MetadataDirective.COPY)
                    .metadata(Collections.singletonMap("update-path", "true"))
                    .build();

            // Execute the copy-object operation which effectively moves the object
            s3Client.copyObject(copyObjectRequest);

            // Return the new projection to confirm the move
            return newProjection;
        } catch (S3Exception e) {
            if (e.statusCode() == HttpStatus.CONFLICT.value()) {
                throw new DataFileException.AlreadyExists(newProjection + " file already exists");
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isPublished(String projection) {
        try (S3Client s3Client = createS3Client()) {
            GetObjectAclRequest request = GetObjectAclRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .build();

            GetObjectAclResponse response = s3Client.getObjectAcl(request);
            return response.grants().stream().anyMatch(grant ->
                    grant.permission().toString().equalsIgnoreCase("READ") &&
                            grant.grantee().type().toString().equalsIgnoreCase("Group") &&
                            grant.grantee().uri().contains("http://acs.amazonaws.com/groups/global/AllUsers"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void
    publishFile(String projection) {
        try (S3Client s3Client = createS3Client()) {
            PutObjectAclRequest request = PutObjectAclRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .acl("public-read")
                    .build();

            s3Client.putObjectAcl(request);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void
    unPublishFile(String projection) {
        try (S3Client s3Client = createS3Client()) {
            PutObjectAclRequest request = PutObjectAclRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .acl("private")
                    .build();

            PutObjectAclResponse putObjectAclResponse = s3Client.putObjectAcl(request);
            System.out.println(putObjectAclResponse.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Grant> getDcc(String projection) {
        try (S3Client s3Client = createS3Client()) {
            GetObjectAclRequest request = GetObjectAclRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .build();
            GetObjectAclResponse response = s3Client.getObjectAcl(request);
            return response.grants();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void grantDcc(String projection, String dccHubName) {
        try (S3Client s3Client = createS3Client()) {
            PutObjectAclRequest request = PutObjectAclRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .grantRead("id=" + dccHubName)
                    .build();
            s3Client.putObjectAcl(request);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void revokeDcc(String projection, String dccHubName) {
        try (S3Client s3Client = createS3Client()) {
            // Get the current ACLs for the object
            GetObjectAclRequest getAclRequest = GetObjectAclRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .build();
            GetObjectAclResponse aclResponse = s3Client.getObjectAcl(getAclRequest);

            // Filter out the grant for the specified grantee ID
            List<Grant> modifiedGrants = aclResponse.grants().stream()
                    .filter(grant -> !(grant.grantee().id().equals(dccHubName) && grant.permission() == Permission.READ))
                    .collect(Collectors.toList());

            // If all grants are removed, this might result in an inaccessible object; handle this case as needed.
            // Build a new ACL request without the revoked grantee
            AccessControlPolicy newAclPolicy = AccessControlPolicy.builder()
                    .grants(modifiedGrants)
                    .owner(aclResponse.owner())
                    .build();
            PutObjectAclRequest putAclRequest = PutObjectAclRequest.builder()
                    .bucket(s3BucketName)
                    .key(projection)
                    .accessControlPolicy(newAclPolicy)
                    .build();
            s3Client.putObjectAcl(putAclRequest);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private S3Client createS3Client() {
        return S3Client.builder().endpointOverride(URI.create(serviceUrl))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(StaticCredentialsProvider.
                        create(AwsBasicCredentials.create(usernameS3, tokenS3)))
                .overrideConfiguration(
                        ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.none()).build())
                .region(SIGNING_REGION).build();
    }

    // TODO: remove
    public static class
    ObjectNotFoundException extends IOException {
        private static final long serialVersionUID = 1L;


        public ObjectNotFoundException() {
            super();
        }


        public ObjectNotFoundException(String message, Throwable cause) {
            super(message, cause);
        }


        public ObjectNotFoundException(String message) {
            super(message);
        }


        public ObjectNotFoundException(Throwable cause) {
            super(cause);
        }
    }

    // TODO: remove
    public static class
    ErrorHandler implements ResponseErrorHandler {
        @Override
        public boolean
        hasError(ClientHttpResponse httpResponse) throws IOException {
            return (httpResponse.getStatusCode().series() == CLIENT_ERROR
                    || httpResponse.getStatusCode().series() == SERVER_ERROR);
        }


        @Override
        public void
        handleError(ClientHttpResponse httpResponse) throws IOException {
            String bodyTest = new BufferedReader(
                    new InputStreamReader(httpResponse.getBody())).lines().collect(Collectors.joining("\n"));
            String detail = bodyTest;
            try {
                JSONObject body = new JSONObject(bodyTest);
                detail = body.optString("detail");
            } catch (JSONException jsone) {
                ;
            }

            switch (httpResponse.getStatusCode()) {
                case UNAUTHORIZED:
                case FORBIDDEN:
                    throw new IOException("AUTH error: " + detail);
                case NOT_FOUND:
                    throw new ObjectNotFoundException(detail);
                case BAD_REQUEST:
                    throw new IOException("Unable to post file: " + detail);
                default:
                    throw new ResourceAccessException("Something wrong: " + detail);
            }
        }
    }
}