package org.impress.storage.scripts

import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.exception.OStorageException
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OSchema
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.metadata.security.ORole
import com.orientechnologies.orient.core.metadata.security.OSecurity
import com.tinkerpop.blueprints.Parameter
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import com.tinkerpop.blueprints.impls.orient.OrientVertexType
import groovy.util.logging.Slf4j


@Slf4j
class CreateSchema {

    static createSchemaInDatabase(String dbName) {

        def admin = new OServerAdmin('remote:localhost/' + dbName)
        admin.connect("root", "root")

        try {
            if (admin.existsDatabase('local'))
                log.info('Database [ ' + dbName + '] already exists.')
        }
        catch (OStorageException e1) {
            admin = new OServerAdmin('remote:localhost/' + dbName)
            admin.connect("root", "root")
            log.info "Creating database..."
            admin.createDatabase(dbName, "graph", "plocal")
            admin.close()
            log.info("Created Database [ " + dbName + "].")
        }

        OrientGraphNoTx graph = new OrientGraphNoTx("remote:localhost/" + dbName)

        try {
            OSecurity sm = graph.getRawGraph().getMetadata().getSecurity()
            ORole writer = sm.getRole("writer")
            ORole reader = sm.getRole("reader")

            // applying old setup (pre-orient2.1) to roles
            [writer, reader].each{ role ->
                role.addRule('database.cluster.orole',2)
                role.addRule('database.class.ouser',2)
                role.addRule('database.cluster.ouser',2)
                role.save()
            }

            sm.createUser("support", "support", reader)
            sm.createUser("test", "123", writer, reader)
            sm.createUser("ufpe", "123", writer, reader)

            log.info("Added roles to OUsers.")

            OSchema schema = graph.getRawGraph().getMetadata().getSchema()

            OClass restrictedType = schema.getClass("ORestricted")
            graph.getVertexType("V").setSuperClass(restrictedType)
            graph.getEdgeType("E").setSuperClass(restrictedType)

            log.info("Created OrientVertexType MeasurementVariable")

            OClass sampleType = schema.createClass("Sample")
            sampleType.createProperty("value", OType.DOUBLE)
            sampleType.createProperty("timestamp", OType.DATETIME)

            log.info("Created OClass Sample")

            OClass samplesType = schema.createClass("Samples")
            samplesType.createProperty("variable", OType.LINK)
            samplesType.createProperty("samples", OType.EMBEDDEDLIST, sampleType)

            log.info("Created OClass Samples")

            OClass logType = schema.createClass("Log")
            logType.createProperty("sum", OType.LINKMAP, sampleType)
            logType.createProperty("mean", OType.LINKMAP, sampleType)
            logType.createProperty("timestamp", OType.DATETIME)

            log.info("Created OClass Log")

            OClass minuteType = schema.createClass("Minute")
            minuteType.createProperty("log", OType.LINK, logType)
            minuteType.createProperty("sample", OType.LINKMAP, samplesType)
            minuteType.createProperty("lastMeasurement", OType.LINK, minuteType)

            log.info("Created OClass Minute")

            OClass hourType = schema.createClass("Hour")
            hourType.createProperty("log", OType.LINK, logType)
            hourType.createProperty("minute", OType.LINKMAP, minuteType)
            hourType.createProperty("lastMeasurement", OType.LINK, hourType)

            log.info("Created OClass Hour")

            OClass dayType = schema.createClass("Day")
            dayType.createProperty("log", OType.LINK, logType)
            dayType.createProperty("hour", OType.LINKMAP, hourType)
            dayType.createProperty("lastMeasurement", OType.LINK, dayType)

            log.info("Created OClass Day")

            OClass monthType = schema.createClass("Month")
            monthType.createProperty("log", OType.LINK, logType)
            monthType.createProperty("day", OType.LINKMAP, dayType)
            monthType.createProperty("lastMeasurement", OType.LINK, monthType)

            log.info("Created OClass Month")

            OClass yearType = schema.createClass("Year")
            yearType.createProperty("log", OType.LINK, logType)
            yearType.createProperty("month", OType.LINKMAP, monthType)
            yearType.createProperty("lastMeasurement", OType.LINK, yearType)

            log.info("Created OClass Year")

            OClass measurementsType = schema.createClass("Measurements")
            measurementsType.createProperty("year", OType.LINKMAP, yearType)

            log.info("Created OClass Measurements")

            OClass fakeResourceType = schema.createClass("FakeResource")
            fakeResourceType.createProperty("domainData", OType.EMBEDDEDMAP)

            log.info("Created OClass FakeResource")

            OrientVertexType measurementVariableType = graph.createVertexType("MeasurementVariable")
            measurementVariableType.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true)
            measurementVariableType.createProperty("unit", OType.STRING).setMandatory(true).setNotNull(true)
            graph.createKeyIndex("name", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "MeasurementVariable"))

            OrientVertexType areaType = graph.createVertexType("Area")
            areaType.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true)
            areaType.createProperty("domainData", OType.EMBEDDEDMAP)
            graph.createKeyIndex("name", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Area"))

            log.info("Created OrientVertexType Area")

            OrientVertexType groupType = graph.createVertexType("Group")
            groupType.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true)
            groupType.createProperty("domainData", OType.EMBEDDEDMAP)
            graph.createKeyIndex("name", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Group"))

            log.info("Created OrientVertexType Group")

            OrientVertexType simulationType = graph.createVertexType("Simulation")
            simulationType.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true)
            simulationType.createProperty("domainData", OType.EMBEDDEDMAP)
            simulationType.createProperty("fakeAreaResources", OType.EMBEDDEDSET)
            simulationType.createProperty("fakeGroupResources", OType.EMBEDDEDSET)
            graph.createKeyIndex("name", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Simulation"))

            log.info("Created OrientVertexType Simulation")

            OrientVertexType resourceType = graph.createVertexType("Resource")
            resourceType.createProperty("networkId", OType.STRING).setMandatory(true).setNotNull(true)
            resourceType.createProperty("domainData", OType.EMBEDDEDMAP)
            resourceType.createProperty("measurements", OType.LINK, measurementsType)
            graph.createKeyIndex("networkId", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Resource"))

            log.info("Created OrientVertexType Resource")

            OrientEdgeType areaHasAreaType = graph.createEdgeType("HasArea")
            areaHasAreaType.createProperty("in", OType.LINK, areaType)
            areaHasAreaType.createProperty("out", OType.LINK, areaType)

            log.info("Created OrientEdgeType HasArea")

            OrientEdgeType deviceHasMeasurementsType = graph.createEdgeType("HasMeasurements")
            deviceHasMeasurementsType.createProperty("in", OType.LINK, measurementsType)
            deviceHasMeasurementsType.createProperty("out", OType.LINK, resourceType)

            log.info("Created OrientEdgeType HasMeasurements")

            OrientEdgeType areaHasDeviceType = graph.createEdgeType("HasResource")
            areaHasDeviceType.createProperty("in", OType.LINK, resourceType)
            areaHasDeviceType.createProperty("out", OType.LINK, areaType)

            log.info("Created OrientEdgeType HasResource")

            OrientEdgeType groupHasDeviceType = graph.createEdgeType("GroupsResource")
            groupHasDeviceType.createProperty("in", OType.LINK, resourceType)
            groupHasDeviceType.createProperty("out", OType.LINK, groupType)

            log.info("Created OrientEdgeType GroupsResource")

            OrientEdgeType simulationHasDeviceType = graph.createEdgeType("SimulatesResource")
            simulationHasDeviceType.createProperty("in", OType.LINK, resourceType)
            simulationHasDeviceType.createProperty("out", OType.LINK, simulationType)

            log.info("Created OrientEdgeType SimulatesResource")

            OrientEdgeType simulationHasAreaType = graph.createEdgeType("SimulatesArea")
            simulationHasAreaType.createProperty("in", OType.LINK, areaType)
            simulationHasAreaType.createProperty("out", OType.LINK, simulationType)

            log.info("Created OrientEdgeType SimulatesArea")

            OrientEdgeType simulationHasGroupType = graph.createEdgeType("SimulatesGroup")
            simulationHasGroupType.createProperty("in", OType.LINK, groupType)
            simulationHasGroupType.createProperty("out", OType.LINK, simulationType)

            log.info("Created OrientEdgeType SimulatesGroup")

            OrientEdgeType simulationIncludesDeviceType = graph.createEdgeType("IncludesResource")
            simulationIncludesDeviceType.createProperty("in", OType.LINK, resourceType)
            simulationIncludesDeviceType.createProperty("out", OType.LINK, simulationType)

            log.info("Created OrientEdgeType IncludesResource")

            OrientEdgeType canMonitorType = graph.createEdgeType("CanMonitor")
            canMonitorType.createProperty("in", OType.LINK, resourceType)
            canMonitorType.createProperty("out", OType.LINK, measurementVariableType)

            log.info("Created OrientEdgeType CanMonitor")

            OrientEdgeType canActuateType = graph.createEdgeType("CanActuate")
            canActuateType.createProperty("in", OType.LINK, resourceType)
            canActuateType.createProperty("out", OType.LINK, measurementVariableType)

            log.info("Created OrientEdgeType CanActuate")

            OrientEdgeType deviceCanMeasureType = graph.createEdgeType("CanMeasure")
            deviceCanMeasureType.createProperty("in", OType.LINK, measurementVariableType)
            deviceCanMeasureType.createProperty("out", OType.LINK, resourceType)

            log.info("Created OrientEdgeType CanMeasure")

            graph.commit()
        } finally {
            graph.shutdown()
        }
    }
}

CreateSchema.createSchemaInDatabase('iot')
CreateSchema.createSchemaInDatabase('test')