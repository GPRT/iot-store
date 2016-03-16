g = new OrientGraph("remote:localhost/iot")

ufpeUser = g.getRawGraph().browseClass("OUser").find{it.field("name") == "ufpe"}
ufamUser = g.getRawGraph().browseClass("OUser").find{it.field("name") == "ufam"}
ufpeReaderUser = g.getRawGraph().browseClass("OUser").find{it.field("name") == "ufpe_reader"}

edgeClasses = [
    "CanActuate",
    "CanMeasure",
    "CanMonitor",
    "GroupsResource",
    "HasArea",
    "HasMeasurements",
    "HasResource",
    "IncludesResource",
    "SimulatesArea",
    "SimulatesGroup",
    "SimulatesResource"]

edges = edgeClasses.collect{
    edgeClass ->
        g.getEdgesOfClass(edgeClass).collect{
            it.getRecord().field("_allow")}
}.sum()

db.commit()