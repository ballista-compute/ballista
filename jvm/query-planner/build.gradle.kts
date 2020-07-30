description = "Ballista query planner"

plugins {
    kotlin("plugin.serialization") version "1.3.61"
}

dependencies {

    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":physical-plan"))
    implementation(project(":optimizer"))

    implementation("org.apache.arrow:arrow-memory:0.17.0")
    implementation("org.apache.arrow:arrow-vector:0.17.0")
}
