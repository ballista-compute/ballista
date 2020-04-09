plugins {
    `maven-publish`
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "org.ballistacompute"
            artifactId = "logical-plan"
            version = "0.2.0-SNAPSHOT"

            from(components["kotlin"])
        }
    }
}

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))

    implementation("org.apache.arrow:arrow-memory:0.16.0")
    implementation("org.apache.arrow:arrow-vector:0.16.0")

}
