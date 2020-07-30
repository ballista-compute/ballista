description = "Ballista logical plan"

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))

    implementation("org.apache.arrow:arrow-memory:0.17.0")
    implementation("org.apache.arrow:arrow-vector:0.17.0")

}
