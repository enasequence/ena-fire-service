package org.example;

public abstract class DataFileException extends RuntimeException {
    public DataFileException(String message) {super(message);}

    public DataFileException(Exception exception) {super(exception);}

    public static class AlreadyExists extends DataFileException {
        public AlreadyExists(String message) {super(message);}
    }

    public static class Md5Mismatch extends DataFileException {
        public Md5Mismatch(String message) {super(message);}
    }

    public static class UnknownError extends DataFileException {
        public UnknownError(String message) {super(message);}

        public UnknownError(Exception exception) {super(exception);}
    }

    public static class InaccessibleFile extends DataFileException {
        public InaccessibleFile(String message) {super(message);}
    }

    public static class PermanentError extends DataFileException {
        public PermanentError(String message) {super(message);}
    }

    public static class DownloadFailure extends DataFileException {
        public DownloadFailure(String message) {super(message);}

        public DownloadFailure(Exception exception) {super(exception);}
    }

    public static class NoFirePath extends DataFileException {
        public NoFirePath(String message) {super(message);}
    }

    public static class UploadMd5Mismatch extends DataFileException {
        public UploadMd5Mismatch(String message) {super(message);}
    }
}
