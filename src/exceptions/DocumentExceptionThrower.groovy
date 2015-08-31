package exceptions

interface DocumentExceptionThrower {
    abstract void invalidDocumentProperties()
    abstract void documentNotFoundById()
    abstract void vertexNotFoundById(Long id)
}
