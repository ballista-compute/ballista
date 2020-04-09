plugins {
    `maven-publish`
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "org.ballistacompute"
            artifactId = "datasource"
            version = "0.2.0-SNAPSHOT"

            from(components["kotlin"])
        }
    }
}
dependencies {

    implementation(project(":datatypes"))

    implementation("org.apache.arrow:arrow-memory:0.16.0")
    implementation("org.apache.arrow:arrow-vector:0.16.0")

    implementation("org.apache.hadoop:hadoop-common:3.1.0")
    implementation("org.apache.parquet:parquet-arrow:1.11.0")
    implementation("org.apache.parquet:parquet-common:1.11.0")
    implementation("org.apache.parquet:parquet-column:1.11.0")
    implementation("org.apache.parquet:parquet-hadoop:1.11.0")
}
