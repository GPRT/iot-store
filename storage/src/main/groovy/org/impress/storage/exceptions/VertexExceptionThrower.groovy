package org.impress.storage.exceptions

interface VertexExceptionThrower {
    abstract void duplicatedVertex()
    abstract void invalidVertexProperties()
    abstract void vertexNotFoundById(Long id)
}
