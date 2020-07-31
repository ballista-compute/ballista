description = "Ballista client"

dependencies {

    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":protobuf"))

    implementation("org.apache.arrow:flight-core:0.17.0")
    implementation("org.apache.arrow:flight-grpc:0.17.0")

}


tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
