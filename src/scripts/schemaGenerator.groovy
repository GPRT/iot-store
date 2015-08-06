package scripts

import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OSchema
import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.Parameter
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import com.tinkerpop.blueprints.impls.orient.OrientVertexType

OrientGraphFactory factory = new OrientGraphFactory("remote:localhost/iot").setupPool(1, 10)

OrientGraphNoTx graph = factory.getNoTx()

try {
    OSchema schema = graph.getRawGraph().getMetadata().getSchema()

    OClass restrictedType = schema.getClass("ORestricted")
    graph.getVertexType("V").setSuperClass(restrictedType)
    graph.getEdgeType("E").setSuperClass(restrictedType)

    OrientVertexType measurementVariableType = graph.createVertexType("MeasurementVariable")
    measurementVariableType.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true)
    measurementVariableType.createProperty("unit", OType.STRING).setMandatory(true).setNotNull(true)
    graph.createKeyIndex("name", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "MeasurementVariable"))

    OClass sampleType = schema.createClass("Sample")
    sampleType.createProperty("timestamp", OType.DATETIME)
    sampleType.createProperty("measurementVariable", OType.EMBEDDED, measurementVariableType)
    sampleType.createProperty("value", OType.DOUBLE)

    OClass minuteType = schema.createClass("Minute")
    minuteType.createProperty("log", OType.EMBEDDEDSET, sampleType)
    minuteType.createProperty("sample", OType.EMBEDDEDLIST, sampleType)

    OClass hourType = schema.createClass("Hour")
    hourType.createProperty("log", OType.EMBEDDEDSET, minuteType)
    hourType.createProperty("minute", OType.LINKMAP, minuteType)

    OClass dayType = schema.createClass("Day")
    dayType.createProperty("log", OType.EMBEDDEDSET, sampleType)
    dayType.createProperty("hour", OType.LINKMAP, hourType)

    OClass monthType = schema.createClass("Month")
    monthType.createProperty("log", OType.EMBEDDEDSET, sampleType)
    monthType.createProperty("day", OType.LINKMAP, dayType)

    OClass yearType = schema.createClass("Year")
    yearType.createProperty("log", OType.EMBEDDEDSET, sampleType)
    yearType.createProperty("month", OType.LINKMAP, monthType)

    OClass measurementsType = schema.createClass("Measurements")
    measurementsType.createProperty("year", OType.LINKMAP, yearType)

    OrientVertexType areaType = graph.createVertexType("Area")
    areaType.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true)
    areaType.createProperty("domainData", OType.EMBEDDEDMAP)
    graph.createKeyIndex("name", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Area"))

    OrientVertexType groupType = graph.createVertexType("Group")
    groupType.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true)
    groupType.createProperty("domainData", OType.EMBEDDEDMAP)
    graph.createKeyIndex("name", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Group"))

    OClass fakeResourceType = schema.createClass("FakeResource")
    fakeResourceType.createProperty("domainData", OType.EMBEDDEDMAP)

    OrientVertexType simulationType = graph.createVertexType("Simulation")
    simulationType.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true)
    simulationType.createProperty("domainData", OType.EMBEDDEDMAP)
    simulationType.createProperty("fakeAreaResources", OType.EMBEDDEDSET)
    simulationType.createProperty("fakeGroupResources", OType.EMBEDDEDSET)
    graph.createKeyIndex("name", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Simulation"))

    OrientVertexType resourceType = graph.createVertexType("Resource")
    resourceType.createProperty("networkId", OType.STRING).setMandatory(true).setNotNull(true)
    resourceType.createProperty("domainData", OType.EMBEDDEDMAP)
    resourceType.createProperty("measurements", OType.LINK, measurementsType)
    graph.createKeyIndex("networkId", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "Resource"))

    OrientEdgeType areaHasAreaType = graph.createEdgeType("HasArea")
    areaHasAreaType.createProperty("in", OType.LINK, areaType)
    areaHasAreaType.createProperty("out", OType.LINK, areaType)

    OrientEdgeType deviceHasMeasurementsType = graph.createEdgeType("HasMeasurements")
    deviceHasMeasurementsType.createProperty("in", OType.LINK, measurementsType)
    deviceHasMeasurementsType.createProperty("out", OType.LINK, resourceType)

    OrientEdgeType areaHasDeviceType = graph.createEdgeType("HasResource")
    areaHasDeviceType.createProperty("in", OType.LINK, resourceType)
    areaHasDeviceType.createProperty("out", OType.LINK, areaType)

    OrientEdgeType groupHasDeviceType = graph.createEdgeType("GroupsResource")
    groupHasDeviceType.createProperty("in", OType.LINK, resourceType)
    groupHasDeviceType.createProperty("out", OType.LINK, groupType)

    OrientEdgeType simulationHasDeviceType = graph.createEdgeType("SimulatesResource")
    simulationHasDeviceType.createProperty("in", OType.LINK, resourceType)
    simulationHasDeviceType.createProperty("out", OType.LINK, simulationType)

    OrientEdgeType simulationHasAreaType = graph.createEdgeType("SimulatesArea")
    simulationHasAreaType.createProperty("in", OType.LINK, areaType)
    simulationHasAreaType.createProperty("out", OType.LINK, simulationType)

    OrientEdgeType simulationHasGroupType = graph.createEdgeType("SimulatesGroup")
    simulationHasGroupType.createProperty("in", OType.LINK, groupType)
    simulationHasGroupType.createProperty("out", OType.LINK, simulationType)

    OrientEdgeType simulationExcludesDeviceType = graph.createEdgeType("ExcludesResource")
    simulationExcludesDeviceType.createProperty("in", OType.LINK, resourceType)
    simulationExcludesDeviceType.createProperty("out", OType.LINK, simulationType)

    OrientEdgeType canMonitorType = graph.createEdgeType("CanMonitor")
    canMonitorType.createProperty("in", OType.LINK, resourceType)
    canMonitorType.createProperty("out", OType.LINK, measurementVariableType)

    OrientEdgeType canActuateType = graph.createEdgeType("CanActuate")
    canActuateType.createProperty("in", OType.LINK, resourceType)
    canActuateType.createProperty("out", OType.LINK, measurementVariableType)

    graph.commit()
} finally {
    graph.commit();
//    graph.shutdown();
}

