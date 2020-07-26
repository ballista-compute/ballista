description = "Ballista data types"

dependencies {
    implementation("org.apache.arrow:arrow-memory:1.0.0")
    implementation("org.apache.arrow:arrow-vector:1.0.0")
    implementation(project(":logical-plan"))
    implementation(project(":physical-plan"))
}
