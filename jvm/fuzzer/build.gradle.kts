description = "Ballista logical query plan fuzzer utility"

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))

    implementation("org.apache.arrow:arrow-vector:1.0.0")
}
