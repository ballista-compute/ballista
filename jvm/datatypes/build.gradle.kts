plugins {
    `maven-publish`
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "org.ballistacompute"
            artifactId = "datatypes"
            version = "0.2.0-SNAPSHOT"

            from(components["kotlin"])
        }
    }
}
dependencies {

    implementation("org.apache.arrow:arrow-memory:0.16.0")
    implementation("org.apache.arrow:arrow-vector:0.16.0")
}
