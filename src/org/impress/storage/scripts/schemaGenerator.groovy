package org.impress.storage.scripts

import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OSchema
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.metadata.security.OSecurity
import com.tinkerpop.blueprints.Parameter
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import com.tinkerpop.blueprints.impls.orient.OrientVertexType

OrientGraphFactory factory = new OrientGraphFactory("remote:localhost/iot").setupPool(1, 10)

OrientGraphNoTx graph = factory.getNoTx()

OSecurity sm = graph.getRawGraph().getMetadata().getSecurity()
sm.createUser("support", "support", sm.getRole("reader"))
sm.createUser("test", "123", sm.getRole("writer"), sm.getRole("reader"))
sm.createUser("ufpe", "123", sm.getRole("writer"), sm.getRole("reader"))
sm.createUser("ufam", "123", sm.getRole("writer"), sm.getRole("reader"))
sm.createUser("fit", "123", sm.getRole("writer"), sm.getRole("reader"))

try {
    OSchema schema = graph.getRawGraph().getMetadata().getSchema()

    OClass restrictedType = schema.getClass("ORestricted")
    graph.getVertexType("V").setSuperClass(restrictedType)
    graph.getEdgeType("E").setSuperClass(restrictedType)

    OrientVertexType measurementVariableType = graph.createVertexType("MeasurementVariable")
    measurementVariableType.createProperty("name", OType.STRING).setMandatory(true).setNotNull(true)
    measurementVariableType.createProperty("unit", OType.STRING).setMandatory(true).setNotNull(true)
    graph.createKeyIndex("name", Vertex.class, new Parameter("type", "UNIQUE"), new Parameter("class", "MeasurementVariable"))

    OClass sampleType = schema.createClass("Sample", restrictedType)
    sampleType.createProperty("timestamp", OType.DATETIME)
    sampleType.createProperty("measurementVariable", OType.LINK, measurementVariableType)
    sampleType.createProperty("value", OType.DOUBLE)

    OClass logType = schema.createClass("Log", restrictedType)
    logType.createProperty("sum", OType.LINKMAP, sampleType)
    logType.createProperty("mean", OType.LINKMAP, sampleType)
    logType.createProperty("timestamp", OType.DATETIME)

    OClass minuteType = schema.createClass("Minute", restrictedType)
    minuteType.createProperty("log", OType.LINK, logType)
    minuteType.createProperty("sample", OType.LINKLIST)
    minuteType.createProperty("lastMeasurement", OType.LINK, minuteType)

    OClass hourType = schema.createClass("Hour", restrictedType)
    hourType.createProperty("log", OType.LINK, logType)
    hourType.createProperty("minute", OType.LINKMAP, minuteType)
    hourType.createProperty("lastMeasurement", OType.LINK, hourType)

    OClass dayType = schema.createClass("Day", restrictedType)
    dayType.createProperty("log", OType.LINK, logType)
    dayType.createProperty("hour", OType.LINKMAP, hourType)
    dayType.createProperty("lastMeasurement", OType.LINK, dayType)

    OClass monthType = schema.createClass("Month", restrictedType)
    monthType.createProperty("log", OType.LINK, logType)
    monthType.createProperty("day", OType.LINKMAP, dayType)
    monthType.createProperty("lastMeasurement", OType.LINK, monthType)

    OClass yearType = schema.createClass("Year", restrictedType)
    yearType.createProperty("log", OType.LINK, logType)
    yearType.createProperty("month", OType.LINKMAP, monthType)
    yearType.createProperty("lastMeasurement", OType.LINK, yearType)

    OClass measurementsType = schema.createClass("Measurements", restrictedType)
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

    OrientEdgeType simulationIncludesDeviceType = graph.createEdgeType("IncludesResource")
    simulationIncludesDeviceType.createProperty("in", OType.LINK, resourceType)
    simulationIncludesDeviceType.createProperty("out", OType.LINK, simulationType)

    OrientEdgeType canMonitorType = graph.createEdgeType("CanMonitor")
    canMonitorType.createProperty("in", OType.LINK, resourceType)
    canMonitorType.createProperty("out", OType.LINK, measurementVariableType)

    OrientEdgeType canActuateType = graph.createEdgeType("CanActuate")
    canActuateType.createProperty("in", OType.LINK, resourceType)
    canActuateType.createProperty("out", OType.LINK, measurementVariableType)

    graph.commit()
} finally {
    graph.shutdown();
}

