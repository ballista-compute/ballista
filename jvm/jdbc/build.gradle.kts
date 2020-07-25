description = "Ballista JDBC Driver"

plugins {
    java
}

dependencies {

    implementation("org.apache.arrow:flight-core:1.0.0")
    implementation("org.apache.arrow:flight-grpc:1.0.0")

    testImplementation("junit:junit:4.13")
}

tasks.test {
    //useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
