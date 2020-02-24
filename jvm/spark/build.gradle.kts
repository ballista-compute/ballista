plugins {
    scala
}

dependencies {

    compile(project(":jdbc"))

    implementation("org.apache.spark:spark-core_2.12:2.4.5")
    implementation("org.apache.spark:spark-sql_2.12:2.4.5")

    testImplementation("junit:junit:4.12")
}
