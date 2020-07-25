description = "Ballista query optimizer"

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))

    implementation("org.apache.arrow:arrow-memory:1.0.0")
    implementation("org.apache.arrow:arrow-vector:1.0.0")

}
