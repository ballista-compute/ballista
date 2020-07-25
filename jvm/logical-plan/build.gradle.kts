description = "Ballista logical plan"

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))

    implementation("org.apache.arrow:arrow-memory:1.0.0")
    implementation("org.apache.arrow:arrow-vector:1.0.0")

}
