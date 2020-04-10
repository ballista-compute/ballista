plugins {
    scala
    kotlin("jvm") version "1.3.50" apply false
}
allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
        jcenter()
    }
}

apply {
    plugin("org.jetbrains.kotlin.jvm")
}

dependencies {

    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    // note that this project depends on the kotlin artifacts being published to a local maven repository
    // see ../jvm/README.md for instructions on publishing those artifacts
    implementation("org.ballistacompute:datatypes:0.2.0-SNAPSHOT")
    implementation("org.ballistacompute:datasource:0.2.0-SNAPSHOT")
    implementation("org.ballistacompute:logical-plan:0.2.0-SNAPSHOT")

    implementation("org.apache.arrow:flight-core:0.16.0")
    implementation("org.apache.arrow:flight-grpc:0.16.0")

    implementation("org.apache.spark:spark-core_2.12:3.0.0-preview2")
    implementation("org.apache.spark:spark-sql_2.12:3.0.0-preview2")

    testImplementation("junit:junit:4.12")
}
