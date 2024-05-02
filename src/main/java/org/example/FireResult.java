package org.example;

public enum FireResult {
    OK(0L, ""),
    UNKNOWN(1L, "Invalid request type: "),
    INVALID_FIRE_OID(2L, "Invalid FIRE OID: "),
    NO_OBJECT(3L, "Object doesn't exist: "),
    INVALID_PATH(4L, "Invalid path: "),
    NO_STAGING_FILE(5L, "File doesn't exist on staging area: "),
    INVALID_SIZE(6L, "Invalid file size: "),
    SIZE_MISMATCH(7L, "File size mismatch: "),
    INVALID_MD5(8L, "Invalid file MD5: "),
    MD5_MISMATCH(9L, "File MD5 mismatch: "),
    ALREADY_EXISTS(12L, "File or directory already exists: ");

    public Long code;
    public String reasonPrefix;

    FireResult(Long code, String reasonPrefix) {
        this.code = code;
        this.reasonPrefix = reasonPrefix;
    }
}
