plugins {
    kotlin("plugin.serialization") version "1.3.61"
    scala
    application
}

application {
    mainClassName = "org.ballistacompute.benchmarks.Benchmarks"
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

    implementation("org.apache.arrow:arrow-vector:0.17.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.4")
}
