package exceptions

import exceptions.ResponseErrorCode
import utils.JsonTransformer

class ResponseErrorException extends RuntimeException {
    ResponseErrorCode errorCode
    int statusCode
    String message
    String description

    ResponseErrorException(ResponseErrorCode errorCode, int statusCode, String message, String description)
    {
        super(message)
        this.errorCode = errorCode
        this.statusCode = statusCode
        this.message = message
        this.description = description
    }

    public int statusCode()
    {
        return statusCode
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
