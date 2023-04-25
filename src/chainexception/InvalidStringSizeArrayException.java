package chainexception;

public class InvalidStringSizeArrayException extends ChainException {
    public InvalidStringSizeArrayException() {
        super();
    }

    public InvalidStringSizeArrayException(Exception ex, String name) {
        super(ex, name);
    }
}
