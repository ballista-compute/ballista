plugins {
    kotlin("plugin.serialization") version "1.3.61"
}


dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":physical-plan"))
    implementation(project(":query-planner"))
    implementation(project(":optimizer"))
    implementation(project(":sql"))

    implementation("org.apache.arrow:arrow-vector:0.16.0")
}
