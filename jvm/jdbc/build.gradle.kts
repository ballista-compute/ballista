description = "Ballista JDBC Driver"

plugins {
    java
}

dependencies {

    implementation("org.apache.arrow:flight-core:0.17.0")
    implementation("org.apache.arrow:flight-grpc:0.17.0")

    testImplementation("junit:junit:4.13")
}

tasks.test {
    //useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
