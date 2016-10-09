import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.exception.OSecurityAccessException
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.id.ORecordId
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.eclipse.jetty.io.EndPoint
import org.impress.storage.exceptions.ResponseErrorCode
import org.impress.storage.exceptions.ResponseErrorException
import org.impress.storage.utils.Endpoints
import org.impress.storage.utils.OrientTransformer
import org.spockframework.compiler.model.Spec
import spock.lang.*
import org.impress.storage.databaseInterfacer.AreaInterfacer
import utils.SpecSetupBuilder

class AreaSpec extends Specification {

    @Shared AreaInterfacer areaInterfacer = new AreaInterfacer()
    @Shared String iotStorePath
    @Shared OrientTransformer oTransformer

    @Shared Map<String,String> paths = [:]
    @Shared Map<String,ORID> currentRids =[:]
    @Shared Map<String,Long> currentPositions = [:]
    @Shared Map<String,Integer> currentClusters = [:]

    @Shared data
    @Shared expected

    @Shared Closure executeTx = { transaction ->
        ODatabaseDocumentTx db = SpecSetupBuilder.getDatabase()
        try {
            OrientGraph graph = new OrientGraph(db)
            return transaction(graph, db)
        }
        finally {
            db.close()
        }
    }

    def setupSpec(){
        iotStorePath = Endpoints.mainUrl
        paths.put('area', SpecSetupBuilder.getPaths('area'))
        paths.put('resource', SpecSetupBuilder.getPaths('resource'))
    }

    def setup(){
        executeTx({ graph, db ->
            OrientVertex area = graph.addVertex("class:Area")
            area.setProperty("name", "area0")
            area.setProperty("domainData", [:])
            area.setProperty("areas", [])
            area.setProperty("parentArea", "")
            area.setProperty("devices", [])
            area.save()
            graph.commit()

            OrientVertex device = graph.addVertex("class:Resource")
            device.setProperty("networkId", "device0")
            device.setProperty("domainData", [:])
            device.setProperty("area", "")
            device.setProperty("groups", [])
            device.save()
            graph.commit()

            [['area', area],['resource', device]].each {
                String className, OrientVertex vertex ->
                    currentRids.put(className, vertex.getIdentity())
                    currentClusters.put(className, vertex.getIdentity().clusterId)
                    currentPositions.put(className, currentRids[className].clusterPosition)
                    Endpoints.addCluster(vertex.getIdentity().clusterId, className)
            }
            areaInterfacer.setDefaultClusterId(db)
        })
        data = ["name"      : "area1",
                "domainData": [:],
                "areas"     : [],
                "parentArea": "",
                "devices"   : []]

        expected = ["id"              : null,
                    "parentArea"      : null,
                    "areas"           : [],
                    "devices"         : [],
                    "name"            : "area1",
                    "domainData"      : [:],
                    "inheritedAreas"  : [],
                    "inheritedDevices": []]

    }

    def "create empty area with domain data"() {
        setup:
        ORID areaRid

        oTransformer = new OrientTransformer()

        executeTx({ graph, db ->
            areaRid = new ORecordId(currentClusters['area'],
                                    currentPositions['area'] + 1)
            URL areaUrl = Endpoints.ridToUrl(areaRid)

            def domainData = ['string':'123',
                          'int':123,
                          'float':1.23]
            data = ["name"      : "area1",
                    "domainData": domainData,
                    "areas"     : [],
                    "parentArea": "",
                    "devices"   : []]

            expected = ["id"              : areaUrl,
                        "parentArea"      : "",
                        "areas"           : [],
                        "devices"         : [],
                        "name"            : "area1",
                        "domainData"      : domainData,
                        "inheritedAreas"  : [],
                        "inheritedDevices": []]
        })

        when:
        def result
        ODatabaseDocumentTx db = SpecSetupBuilder.getDatabase()
        try {
            result = areaInterfacer.create(db, data)
        }
        finally {
            db.close()
        }
        result['domainData'] = result['domainData'].toString()
        expected['domainData'] = expected['domainData'].toString()

        then:
        result == expected
    }

    def "create a nested area"() {
        setup:
        ORID parentRid
        ORID areaRid

        executeTx({ graph, db ->
            OrientVertex parent = graph.getVertex(new ORecordId(currentClusters['area'],
                                                                currentPositions['area']))
            parentRid = parent.getIdentity()

            URL parentUrl = Endpoints.ridToUrl(parentRid)
            areaRid = new ORecordId(parentRid.clusterId, parentRid.clusterPosition + 1)
            URL areaUrl = Endpoints.ridToUrl(areaRid)

            data = ["name"      : "area1",
                    "domainData": [:],
                    "areas"     : [],
                    "parentArea": parentUrl.toString(),
                    "devices"   : []]

            expected = ["id"              : areaUrl,
                        "parentArea"      : parentUrl,
                        "areas"           : [],
                        "devices"         : [],
                        "name"            : "area1",
                        "domainData"      : [:],
                        "inheritedAreas"  : [],
                        "inheritedDevices": []]
        })

        when:
        def result
        ODatabaseDocumentTx db = SpecSetupBuilder.getDatabase()
        try {
            result = areaInterfacer.create(db, data)
        }
        finally {
            db.close()
        }

        then:
        result['parentArea'] == expected['parentArea']
        result['id'] == expected['id']
    }

    def "create area with devices and areas"() {
        setup:
        ORID areaRid
        ORID deviceRid

        executeTx({ graph, db ->

            areaRid = new ORecordId(currentClusters['area'],
                                    currentPositions['area'])
            URL areaUrl = Endpoints.ridToUrl(areaRid)
            deviceRid = new ORecordId(currentClusters['resource'],
                                        currentPositions['resource'])
            URL deviceUrl = Endpoints.ridToUrl(deviceRid)

            data = ["name"      : "area1",
                    "domainData": [:],
                    "parentArea": "",
                    "areas"     : [areaUrl.toString()],
                    "devices"   : [deviceUrl.toString()]]

            expected["areas"] = [areaUrl]
            expected["devices"] = [deviceUrl]
        })

        when:
        def result
        ODatabaseDocumentTx db = SpecSetupBuilder.getDatabase()
        try {
            result = areaInterfacer.create(db, data)
        }
        finally {
            db.close()
        }

        then:
        result['areas'] == expected['areas']
        result['devices'] == expected['devices']
    }

    def "put area with children"() {
        setup:
        ORID areaRid
        ORID deviceRid
        ORID parentRid

        executeTx({ graph, db ->
            OrientVertex parent = graph.addVertex("class:Area")
            parent.setProperty("name", "area1")
            parent.setProperty("domainData", [:])
            parent.setProperty("areas", [])
            parent.setProperty("parentArea", "")
            parent.setProperty("devices", [])
            parent.save()
            graph.commit()


            parentRid = new ORecordId(currentClusters['area'],
                                    currentPositions['area'] + 1)
            URL parentUrl = Endpoints.ridToUrl(parentRid)
            areaRid = new ORecordId(currentClusters['area'],
                    currentPositions['area'])
            URL areaUrl = Endpoints.ridToUrl(areaRid)
            deviceRid = new ORecordId(currentClusters['resource'],
                    currentPositions['resource'])
            URL deviceUrl = Endpoints.ridToUrl(deviceRid)

            data = ["name"      : "area1",
                    "domainData": [:],
                    "parentArea": "",
                    "areas"     : [areaUrl.toString()],
                    "devices"   : [deviceUrl.toString()]]

            expected['id'] = parentUrl
            expected['areas'] = [areaUrl]
            expected['devices'] = [deviceUrl]
        })

        when:
        def result
        ODatabaseDocumentTx db = SpecSetupBuilder.getDatabase()
        try {
            result = areaInterfacer.setById(db, parentRid.clusterPosition, data)
        }
        finally {
            db.close()
        }

        then:
        result['id'] == expected['id']
        result['areas'] == expected['areas']
        result['devices'] == expected['devices']
    }

    def cleanup(){
        ODatabaseDocumentTx db = SpecSetupBuilder.getDatabase()
        try {
            OrientGraph graph = new OrientGraph(db)
            graph.getVertices().each{it.remove()}
            graph.commit()
        }
        finally {
            db.close()
        }
    }

}
