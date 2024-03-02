package exceptions;

public class AWSCredentialsReaderException extends Exception {
    public AWSCredentialsReaderException(String message) {
        super(message, null, false, false);
    }
}
