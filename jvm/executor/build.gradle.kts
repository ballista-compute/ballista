description = "Ballista query executor"

plugins {
    kotlin("plugin.serialization") version "1.3.61"
    application
}

application {
    mainClassName = "org.ballistacompute.executor.Executor"
}

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":physical-plan"))
    implementation(project(":query-planner"))
    implementation(project(":sql"))
    implementation(project(":protobuf"))
    implementation(project(":execution"))

    implementation("org.apache.arrow:arrow-vector:1.0.0")

    implementation("org.apache.arrow:flight-core:1.0.0")
    implementation("org.apache.arrow:flight-grpc:1.0.0")
}
