import org.example.CalculateDigest;
import org.example.DataFileException;
import org.example.FireS3File;
import org.example.FireService;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;


public class
FireServiceTest {
    public static String service_url;
    public static String ENA_USER;
    public static String ENA_PASSWORD;
    public static String ENA_USER_S3;
    public static String ENA_TOKEN_S3;
    public static String DCC1_USER;
    public static String DCC1_TOKEN;
    public static String DCC2_USER;
    public static String DCC2_TOKEN;
    public static String S3_BUCKET_PRIVATE;
    public static String S3_BUCKET_PUBLIC;

    static {
        Properties props = new Properties();
        try (InputStream input
                     = FireServiceTest.class.getClassLoader().getResourceAsStream("config.properties")) {
            props.load(input);
            service_url = props.getProperty("service_url");
            ENA_USER = props.getProperty("ENA_USER");
            ENA_PASSWORD = props.getProperty("ENA_PASSWORD");
            ENA_USER_S3 = props.getProperty("ENA_USER_S3");
            ENA_TOKEN_S3 = props.getProperty("ENA_TOKEN_S3");
            DCC1_USER = props.getProperty("DCC1_USER");
            DCC1_TOKEN = props.getProperty("DCC1_TOKEN");
            DCC2_USER = props.getProperty("DCC2_USER");
            DCC2_TOKEN = props.getProperty("DCC2_TOKEN");
            S3_BUCKET_PRIVATE = props.getProperty("S3_BUCKET_PRIVATE");
            S3_BUCKET_PUBLIC = props.getProperty("S3_BUCKET_PUBLIC");
        } catch (IOException ex) {
            throw new RuntimeException("Error loading properties", ex);
        }
    }

    @Test
    public void
    testUploadAndDownload() throws IOException, NoSuchAlgorithmException {
        FireService service = new FireService(
                service_url, ENA_USER, ENA_PASSWORD,
                ENA_USER_S3, ENA_TOKEN_S3,
                S3_BUCKET_PRIVATE);

        Path file = Files.write(Files.createTempFile("FIRE-POST", "FIRE-POST"),
                "FIRE-POST".getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING);

        String md5 = CalculateDigest.calculateDigest("MD5", file.toFile());
        String projection = Paths.get("ena-file", file.getFileName().toString()).toString();

        long startTime = System.currentTimeMillis(); // Record start time

        service.uploadFile(file, projection);

        long endTime = System.currentTimeMillis(); // Record end time
        long duration = endTime - startTime; // Compute the duration
        System.out.println("Upload duration: " + duration + " ms");
        
        FireS3File fireS3File = service.queryPath(projection);
        Assert.assertNotNull(fireS3File.getFireOid());
        Assert.assertEquals(md5, fireS3File.getMd5());

        Path dnld = service.downloadFile(
                projection,
                Files.createTempFile("FIRE-POST-DNLD", "FIRE-POST-DNLD"));
        Assert.assertEquals(file.toFile().length(), dnld.toFile().length());
        Assert.assertArrayEquals(Files.readAllBytes(file), Files.readAllBytes(dnld));

        Assert.assertTrue(service.deleteFile(fireS3File.getFireOid()));

        Files.deleteIfExists(file);
        Files.deleteIfExists(dnld);
    }

    @Test
    public void
    testPathConflict() throws IOException, NoSuchAlgorithmException {
        FireService service = new FireService(
                service_url, ENA_USER, ENA_PASSWORD,
                ENA_USER_S3, ENA_TOKEN_S3,
                S3_BUCKET_PRIVATE);

        Path file1 = Files.write(Files.createTempFile("FIRE-POST", "FIRE-POST"),
                "FIRE-POST".getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING);
        Path file2 = Files.write(Files.createTempFile("FIRE-POST", "FIRE-POST"),
                "FIRE-POST".getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING);

        String projection = Paths.get("ena-file", file1.getFileName().toString()).toString();

        // first
        service.uploadFile(file1, projection);

        FireS3File fireS3File1 = service.queryPath(projection);
        Assert.assertNotNull(fireS3File1);

        Path dnld1 = service.downloadFile(
                projection,
                Files.createTempFile("FIRE-POST-DNLD", "FIRE-POST-DNLD"));
        Assert.assertEquals(file1.toFile().length(), dnld1.toFile().length());
        Assert.assertArrayEquals(Files.readAllBytes(file1), Files.readAllBytes(dnld1));

        // second
        try {
            service.uploadFile(file2, projection);
            Assert.fail();
        } catch (DataFileException.AlreadyExists ignored) {} // as expected

        FireS3File fireS3File2 = service.queryPath(projection);
        Assert.assertEquals(fireS3File1.getFireOid(), fireS3File2.getFireOid());

        Assert.assertTrue(service.deleteFile(fireS3File1.getFireOid()));
        Assert.assertNull(service.queryPath(projection));

        Files.deleteIfExists(file1);
        Files.deleteIfExists(file2);
        Files.deleteIfExists(dnld1);
    }

    @Test
    public void
    testUploadAndDownloadLargeFileMultiPart() throws IOException, NoSuchAlgorithmException {
        FireService service = new FireService(
                service_url, ENA_USER, ENA_PASSWORD,
                ENA_USER_S3, ENA_TOKEN_S3,
                S3_BUCKET_PRIVATE);

        Path file = createLargeTempFile(8).toPath();

        String projection = Paths.get("ena-file", file.getFileName().toString()).toString();
        String md5 = CalculateDigest.calculateDigest("MD5", file.toFile());

        long startTime = System.currentTimeMillis(); // Record start time

        service.uploadFileMultipart(file, projection);

        long endTime = System.currentTimeMillis(); // Record end time
        long duration = endTime - startTime; // Compute the duration
        System.out.println("Upload duration: " + duration + " ms");

        FireS3File fireS3File = service.queryPath(projection);
        Assert.assertNotNull(fireS3File.getFireOid());
        Assert.assertEquals(md5, fireS3File.getMd5());

        Assert.assertTrue(service.deleteFile(fireS3File.getFireOid()));
        Assert.assertNull(service.queryPath(projection));

        Files.deleteIfExists(file);
    }

    @Test
    public void
    testUploadAndDownloadLargeFileSinglePart() throws IOException, NoSuchAlgorithmException {
        FireService service = new FireService(
                service_url, ENA_USER, ENA_PASSWORD,
                ENA_USER_S3, ENA_TOKEN_S3,
                S3_BUCKET_PRIVATE);

        Path file = createLargeTempFile(8).toPath();

        String projection = Paths.get("ena-file", file.getFileName().toString()).toString();
        String md5 = CalculateDigest.calculateDigest("MD5", file.toFile());

        long startTime = System.currentTimeMillis(); // Record start time

        service.uploadFile(file, projection);

        long endTime = System.currentTimeMillis(); // Record end time
        long duration = endTime - startTime; // Compute the duration
        System.out.println("Upload duration: " + duration + " ms");

        FireS3File fireS3File = service.queryPath(projection);
        Assert.assertNotNull(fireS3File.getFireOid());
        Assert.assertEquals(md5, fireS3File.getMd5());

        Assert.assertTrue(service.deleteFile(fireS3File.getFireOid()));
        Assert.assertNull(service.queryPath(projection));

        Files.deleteIfExists(file);
    }

    @Test
    public void
    testMove() throws IOException, NoSuchAlgorithmException {
        FireService service = new FireService(
                service_url, ENA_USER, ENA_PASSWORD,
                ENA_USER_S3, ENA_TOKEN_S3,
                S3_BUCKET_PRIVATE);

        Path file1 = Files.write(Files.createTempFile("FIRE-POST", "FIRE-POST"),
                "FIRE-POST".getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING);

        String projection1 = Paths.get("ena-file-1", file1.getFileName().toString()).toString();
        String projection2 = Paths.get("ena-file-2", file1.getFileName().toString()).toString();

        service.uploadFile(file1, projection1);
        Assert.assertNotNull(service.queryPath(projection1));
        Assert.assertNull(service.queryPath(projection2));

        service.moveFile(projection1, projection2);
        Assert.assertNotNull(service.queryPath(projection2));
        Assert.assertNull(service.queryPath(projection1));

        Path dnld1 = service.downloadFile(
                projection2,
                Files.createTempFile("FIRE-POST-DNLD", "FIRE-POST-DNLD"));
        Assert.assertEquals(file1.toFile().length(), dnld1.toFile().length());
        Assert.assertArrayEquals(Files.readAllBytes(file1), Files.readAllBytes(dnld1));

        Assert.assertTrue(service.deleteFile(service.queryPath(projection2).getFireOid()));
        Assert.assertNull(service.queryPath(projection2));

        Files.deleteIfExists(file1);
        Files.deleteIfExists(dnld1);
    }

    @Test
    public void
    testPublish() throws IOException, NoSuchAlgorithmException {
        FireService service = new FireService(
                service_url, ENA_USER, ENA_PASSWORD,
                ENA_USER_S3, ENA_TOKEN_S3,
                S3_BUCKET_PRIVATE);

        Path file = Files.write(Files.createTempFile("FIRE-POST", "FIRE-POST"),
                "FIRE-POST".getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING);

        String projection = Paths.get("ena-file", file.getFileName().toString()).toString();
        service.uploadFile(file, projection);

        FireS3File o = service.queryPath(projection);
        Assert.assertNotNull(o.getFireOid());
        Assert.assertEquals(projection, o.getProjection());
        Assert.assertFalse(listFileInBucketAsUser(projection, ENA_USER_S3, ENA_TOKEN_S3, S3_BUCKET_PUBLIC));

        service.publishFile(projection);
        Assert.assertTrue(service.isPublished(projection));
        Assert.assertTrue(listFileInBucketAsUser(projection, ENA_USER_S3, ENA_TOKEN_S3, S3_BUCKET_PUBLIC));
        Path dnld = downloadFileFromBucketAsUser(projection, ENA_USER_S3, ENA_TOKEN_S3, S3_BUCKET_PUBLIC);
        Assert.assertEquals(file.toFile().length(), dnld.toFile().length());
        Assert.assertArrayEquals(Files.readAllBytes(file), Files.readAllBytes(dnld));

        service.unPublishFile(projection);
        Assert.assertFalse(service.isPublished(projection));
        Assert.assertFalse(listFileInBucketAsUser(projection, ENA_USER_S3, ENA_TOKEN_S3, S3_BUCKET_PUBLIC));

        Assert.assertTrue(service.deleteFile(o.getFireOid()));
        Assert.assertNull(service.queryPath(projection));

        Files.deleteIfExists(file);
        Files.deleteIfExists(dnld);
    }

    @Test
    public void
    testDcc() throws IOException, NoSuchAlgorithmException {
        FireService service = new FireService(
                service_url, ENA_USER, ENA_PASSWORD,
                ENA_USER_S3, ENA_TOKEN_S3,
                S3_BUCKET_PRIVATE);

        Assert.assertNull(service.queryPath("no/such/path"));

        Path file = Files.write(Files.createTempFile("FIRE-POST", "FIRE-POST"),
                "FIRE-POST".getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING);

        String projection = Paths.get("ena-file", file.getFileName().toString()).toString();
        service.uploadFile(file, projection);
        Assert.assertNotNull(service.queryPath(projection));
        
        Assert.assertEquals(0, service.getDcc(projection).size());

        Assert.assertFalse(listFileInBucketAsUser(projection, DCC1_USER, DCC1_TOKEN, S3_BUCKET_PRIVATE));
        Assert.assertFalse(listFileInBucketAsUser(projection, DCC2_USER, DCC2_TOKEN, S3_BUCKET_PRIVATE));

        service.grantDcc(projection, DCC1_USER);
        Assert.assertEquals(1, service.getDcc(projection).size());
        Assert.assertTrue(listFileInBucketAsUser(projection, DCC1_USER, DCC1_TOKEN, S3_BUCKET_PRIVATE));
        service.grantDcc(projection, DCC2_USER);
        Assert.assertEquals(2, service.getDcc(projection).size());
        Assert.assertTrue(listFileInBucketAsUser(projection, DCC2_USER, DCC2_TOKEN, S3_BUCKET_PRIVATE));

        service.revokeDcc(projection, DCC1_USER);
        Assert.assertEquals(1, service.getDcc(projection).size());
        Assert.assertFalse(listFileInBucketAsUser(projection, DCC1_USER, DCC1_TOKEN, S3_BUCKET_PRIVATE));
        service.revokeDcc(projection, DCC2_USER);
        Assert.assertEquals(0, service.getDcc(projection).size());
        Assert.assertFalse(listFileInBucketAsUser(projection, DCC2_USER, DCC2_TOKEN, S3_BUCKET_PRIVATE));

        FireS3File o = service.queryPath(projection);
        Assert.assertTrue(service.deleteFile(o.getFireOid()));
        Assert.assertNull(service.queryPath(projection));

        Files.deleteIfExists(file);
    }

    private boolean listFileInBucketAsUser(String projection, String user, String token, String bucket) {
        FireService service = new FireService(
                service_url, ENA_USER, ENA_PASSWORD,
                user, token,
                bucket);
        return service.queryPath(projection) != null;
    }

    private Path downloadFileFromBucketAsUser(String projection, String user, String token, String bucket) throws IOException {
        FireService service = new FireService(
                service_url, ENA_USER, ENA_PASSWORD,
                user, token,
                bucket);

        Path dnld = service.downloadFile(
                projection,
                Files.createTempFile("FIRE-POST-DNLD", "FIRE-POST-DNLD"));

        return dnld;
    }

    private File createLargeTempFile(long sizeMB) throws IOException {
        File tempFile = Files.createTempFile("temp", ".tmp").toFile();
        System.out.println("Temporary file created: " + tempFile.getAbsolutePath());

        // Register the file to be deleted when the JVM exits
        tempFile.deleteOnExit();

        // Set the size of the file to 15MB
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw")) {
            raf.setLength(sizeMB * 1024 * 1024); // 15 MB
        }

        return tempFile;
    }
}
