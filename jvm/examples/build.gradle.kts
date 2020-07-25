plugins {
    kotlin("plugin.serialization") version "1.3.61"
    scala
}


dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":query-planner"))
    implementation(project(":optimizer"))
    implementation(project(":physical-plan"))
    implementation(project(":execution"))
    implementation(project(":sql"))

    implementation("org.apache.arrow:arrow-vector:1.0.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.4")
}
