plugins {
    scala
    kotlin("jvm") version "1.3.50" apply false
}

group = "org.ballistacompute.spark"
version = "0.2.4-SNAPSHOT"
description = "Ballista Spark Support"

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
        jcenter()
    }
}

subprojects {

    apply {
        plugin("org.jetbrains.kotlin.jvm")
    }

    dependencies {

        implementation(kotlin("stdlib-jdk8"))
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
        implementation("org.jetbrains.kotlin:kotlin-reflect")

        implementation("org.rogach:scallop_2.12:3.2.0")

        // note that this project depends on the kotlin artifacts being published to a local maven repository
        // see ../jvm/README.md for instructions on publishing those artifacts
        implementation("org.ballistacompute:datatypes:0.2.4-SNAPSHOT")
        implementation("org.ballistacompute:datasource:0.2.4-SNAPSHOT")
        implementation("org.ballistacompute:logical-plan:0.2.4-SNAPSHOT")
        implementation("org.ballistacompute:protobuf:0.2.4-SNAPSHOT")
        implementation("org.ballistacompute:executor:0.2.4-SNAPSHOT")

        implementation("org.apache.arrow:flight-core:0.17.0")
        implementation("org.apache.arrow:flight-grpc:0.17.0")

        implementation("org.apache.spark:spark-core_2.12:3.0.0-preview2")
        implementation("org.apache.spark:spark-sql_2.12:3.0.0-preview2")

        testImplementation("junit:junit:4.12")

    }
}


