import java.time.Instant

plugins {
    scala
    kotlin("jvm") version "1.3.50" apply false
    id("com.diffplug.gradle.spotless") version "4.4.0"
}

group = "org.ballistacompute.spark"
version = "0.3.0-SNAPSHOT"
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
        plugin("com.diffplug.gradle.spotless")
    }

    spotless {
        scala {
            scalafmt()
        }
    }

    dependencies {

        implementation(kotlin("stdlib-jdk8"))
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
        implementation("org.jetbrains.kotlin:kotlin-reflect")

        implementation("org.rogach:scallop_2.12:3.2.0")

        // note that this project depends on the kotlin artifacts being published to a local maven repository
        // see ../jvm/README.md for instructions on publishing those artifacts
        implementation("org.ballistacompute:datatypes:0.3.0-SNAPSHOT")
        implementation("org.ballistacompute:datasource:0.3.0-SNAPSHOT")
        implementation("org.ballistacompute:logical-plan:0.3.0-SNAPSHOT")
        implementation("org.ballistacompute:protobuf:0.3.0-SNAPSHOT")
        implementation("org.ballistacompute:executor:0.3.0-SNAPSHOT")

        implementation("org.apache.arrow:flight-core:0.17.0")
        implementation("org.apache.arrow:flight-grpc:0.17.0")

        implementation("org.apache.spark:spark-core_2.12:3.0.0")
        implementation("org.apache.spark:spark-sql_2.12:3.0.0")

        testImplementation("junit:junit:4.12")

    }

    tasks.jar {
        manifest {
            attributes(
                "Implementation-Title" to "${rootProject.name}-${archiveBaseName.get()}",
                "Implementation-Version" to rootProject.version,
                "Build-Timestamp" to Instant.now()
            )
        }
    }
}


