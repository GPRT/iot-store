package exceptions

interface ExceptionThrower {
    abstract void duplicatedVertex()
    abstract void invalidVertexProperties()
    abstract void vertexNotFoundById(Long id)
    abstract void vertexNotFoundByIndex(String name)
}
