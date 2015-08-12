package databaseInterfacer

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import utils.OrientTransformer

abstract class ClassInterfacer {
    OrientGraphFactory factory
    OrientTransformer orientTransformer = new OrientTransformer()
    Integer defaultClusterId = -1
    String className = ""
    Map fields = [:]
    Map links = [:]

    def ClassInterfacer(factory, className, fields, links) {
        this.factory = factory
        this.fields = fields
        this.links = links
        this.className = className
        this.defaultClusterId = this.getClusterId(className)
    }

    protected final Number getClusterId(String className) {
        def db = this.factory.getDatabase()
        try {
            return db.getClusterIdByName(className)
        } finally {
            db.close()
        }
    }

    public final Set getFieldNames() {
        return fields.keySet()
    }

    public final Set getExpandedNames() {
        return links.keySet() + fields.keySet()
    }

    protected final String generateQuery(fieldNames=this.fields, Set filterFields=[], Set sortFields=[],
                                         int pageField=0, int pageLimitField=10,
                                         String className=this.className) {
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
}