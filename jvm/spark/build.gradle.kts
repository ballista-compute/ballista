plugins {
    scala
}

dependencies {

    compile(project(":jdbc"))

    implementation("org.apache.spark:spark-core_2.12:3.0.0-preview2")
    implementation("org.apache.spark:spark-sql_2.12:3.0.0-preview2")

    testImplementation("junit:junit:4.12")
}
