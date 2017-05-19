package org.davidmoten.rx.jdbc;

public final class AnnotationsNotFoundException extends SQLRuntimeException{

    private static final long serialVersionUID = 1155711687125951243L;
    
    public AnnotationsNotFoundException(String message) {
        super(message);
    }

}
