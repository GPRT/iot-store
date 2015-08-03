package exceptions

import exceptions.ResponseErrorCode
import utils.JsonTransformer

class ResponseErrorException extends RuntimeException {
    ResponseErrorCode errorCode
    int status
    String message
    String description

    ResponseErrorException(ResponseErrorCode errorCode, int status, String message, String description)
    {
        super(message)
        this.errorCode = errorCode
        this.message = message
        this.description = description
    }

    @Override
    public String toString()
    {
        def map = [:]
        map.errorCode = errorCode
        map.message = message
        map.description = description

        return new JsonTransformer().render(map)
    }
}
