description = "Ballista data sources"

dependencies {

    implementation(project(":datatypes"))

    implementation("org.apache.arrow:arrow-memory:0.17.0")
    implementation("org.apache.arrow:arrow-vector:0.17.0")

    implementation("org.apache.hadoop:hadoop-common:3.1.0")
    implementation("org.apache.parquet:parquet-arrow:1.11.0")
    implementation("org.apache.parquet:parquet-common:1.11.0")
    implementation("org.apache.parquet:parquet-column:1.11.0")
    implementation("org.apache.parquet:parquet-hadoop:1.11.0")

    implementation("com.univocity:univocity-parsers:2.8.4")
}
