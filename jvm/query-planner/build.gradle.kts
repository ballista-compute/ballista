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

    implementation("org.apache.arrow:arrow-memory:1.0.0")
    implementation("org.apache.arrow:arrow-vector:1.0.0")
}
