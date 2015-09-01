package databaseInterfacer

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import exceptions.ResponseErrorCode
import exceptions.ResponseErrorException
import utils.Endpoints
import utils.OrientTransformer

abstract class ClassInterfacer {
    OrientTransformer orientTransformer = new OrientTransformer()
    Integer defaultClusterId = null
    String className = null
    Map fields = [:]
    Map links = [:]

    def ClassInterfacer(className, fields, links) {
        this.fields = fields
        this.links = links
        this.className = className
    }

    abstract protected LinkedHashMap create(ODatabaseDocumentTx db, HashMap data, HashMap optionalData)
    abstract protected LinkedHashMap delete(ODatabaseDocumentTx db, Long id, String className)

    def setDefaultClusterId(ODatabaseDocumentTx db) {
        this.defaultClusterId = db.getClusterIdByName(this.className)
        Endpoints.addCluster(this.defaultClusterId, this.className)
    }

    protected final int getClusterId(ODatabaseDocumentTx db, String className) {
        if (className == this.className)
            return this.defaultClusterId

        return db.getClusterIdByName(className)
    }

    public final Set getFieldNames() {
        return fields.keySet()
    }

    public final Set getExpandedNames() {
        return links.keySet() + fields.keySet()
    }

    protected final String generateQuery(Set fieldNames = this.getExpandedNames(),
                                         Set filterFields = [],
                                         Set sortFields = [],
                                         int pageField = 0,
                                         int pageLimitField = 10,
                                         String className = this.className) {
        def osql = "select from (select @rid as id,"

        osql += fieldNames.collect {
            if (it in this.fields)
                return this.fields[it]
            else
                return this.links[it]
        }.join(", ")

        osql += " from " + className + ")"

        if (!filterFields.isEmpty())
            osql += " where"

        filterFields.eachWithIndex { value, i ->
            def (fieldName, fieldOperator, constant) = value

            osql += " " + fieldName

            switch (fieldOperator) {
                case 'lt':
                    osql += " <"
                    break
                case 'gt':
                    osql += " >"
                    break
                case 'lte':
                    osql += " <="
                    break
                case 'gte':
                    osql += " >="
                    break
                case 'eq':
                    osql += " ="
                    break
                case 'neq':
                    osql += " <>"
                    break
            }

            osql += " " + constant

            if ((i + 1) < filterFields.size())
                osql += " and"
        }

        if (!sortFields.isEmpty())
            osql += " order by"

        sortFields.eachWithIndex { value, i ->
            def (fieldName, fieldOrder) = value
            osql += " " + fieldName + " " + fieldOrder

            if ((i + 1) < sortFields.size())
                osql += " ,"
        }

        osql += " skip " + (pageField * pageLimitField) + " limit " + pageLimitField

        return osql
    }

    protected final Iterable<ODocument> getDocuments(ODatabaseDocumentTx db,
                                                     HashMap params = [:],
                                                     String className = this.className) {
        def osql = generateQuery(params.listFields,
                params.filterFields,
                params.sortFields,
                params.pageField,
                params.pageLimitField, className)

        def query = new OSQLSynchQuery(osql)

        return db.command(query).execute()
    }

    protected final Iterable<ODocument> getDocumentByRid(ODatabaseDocumentTx db,
                                                         ORecordId rid,
                                                         Set fieldNames=this.getExpandedNames()) {
        def osql = generateQuery(fieldNames, [].toSet(), [].toSet(), 0, 10, rid.toString())
        def query = new OSQLSynchQuery(osql)

        return db.command(query).execute()
    }

    protected final OrientVertex getVertexByRid(ODatabaseDocumentTx db,
                                                ORecordId rid) {
        OrientGraphNoTx graph = new OrientGraphNoTx(db)
        return graph.getVertex(rid)
    }

    protected final OrientVertex getVertexByUrl(ODatabaseDocumentTx db,
                                                String url) {
        ORecordId rid = null

        try {
            rid = Endpoints.urlToRid(new URL(url))
        } catch (MalformedURLException e) {
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    400,
                    "[" + url + "] is not a valid url!",
                    "Choose an url that already exists")
        }

        return getVertexByRid(db, rid)
    }

    protected final Iterable<OrientVertex> getVerticesByIndex(ODatabaseDocumentTx db,
                                                              String fieldName,
                                                              String fieldValue,
                                                              String className=this.className) {
        OrientGraphNoTx graph = new OrientGraphNoTx(db)
        def vertices = graph.getVertices(className + "." + fieldName, fieldValue)

        if (!vertices.getAt(0))
            throw new ResponseErrorException(ResponseErrorCode.VALIDATION_ERROR,
                    400,
                    "No field named [$fieldName] was found with value [$fieldValue] in class [$className]!",
                    "Check the body you sent")

        return vertices
    }
}