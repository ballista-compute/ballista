import java.time.Instant

plugins {
    java
    `java-library`
    kotlin("jvm") version "1.3.50" apply false
    `maven-publish`

    //TODO: this has to be uncommented when pushing a release to sonatype but commented out
    // for github actions to work ... would be nice to find a better solution for this
    //id("org.hibernate.build.maven-repo-auth") version "3.0.0"

    id("org.jetbrains.dokka") version "0.10.1"
    signing
}

group = "org.ballistacompute"
version = "0.3.0-SNAPSHOT"

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
        plugin("maven-publish")
        plugin("signing")
        plugin("org.jetbrains.dokka")
    }

    extra["isReleaseVersion"]  = !version.toString().endsWith("SNAPSHOT")

    val implementation by configurations
    val testImplementation by configurations

    dependencies {
        implementation(kotlin("stdlib-jdk8"))

        // Gradle plugin
        implementation(gradleApi())

        // Align versions of all Kotlin components
        implementation(platform("org.jetbrains.kotlin:kotlin-bom"))

        // Use the Kotlin JDK 8 standard library.
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

        // Use the Kotlin test library.
        testImplementation("org.jetbrains.kotlin:kotlin-test")

        // Use the Kotlin JUnit integration.
        testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
        testImplementation("org.junit.jupiter:junit-jupiter:5.5.2")
        testImplementation("junit:junit:4.12")

        implementation("org.jetbrains.kotlinx:kotlinx-serialization-runtime:0.14.0") // JVM dependency
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines:0.19.2")
    }

    tasks.dokka {
        outputFormat = "html"
        outputDirectory = "$buildDir/javadoc"
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

    val sourcesJar = tasks.create<Jar>("sourcesJar") {
        archiveClassifier.set("sources")
        from(sourceSets.getByName("main").allSource)
    }

    val javadocJar = tasks.create<Jar>("javadocJar") {
        archiveClassifier.set("javadoc")
        from("$buildDir/javadoc")
    }

    java {
        withJavadocJar()
    }

    publishing {
        repositories {
            maven {
                name = "sonatype"
                url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2")
                credentials {
                    username = System.getenv("SONATYPE_USERNAME")
                    password = System.getenv("SONATYPE_PASSWORD")
                }
            }
        }

        publications {
            create<MavenPublication>("mavenKotlin") {
                groupId = "org.ballistacompute"
                version = rootProject.version as String?

                pom {
                    name.set("Ballista Compute")
                    description.set("JVM query engine based on Apache Arrow")
                    url.set("https://github.com/ballista-compute/ballista")
                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            id.set("andygrove")
                            name.set("Andy Grove")
                            email.set("andygrove73@gmail.com")
                        }
                    }
                    scm {
                        connection.set("scm:git:git://github.com/ballista-compute/ballista.git")
                        developerConnection.set("scm:git:ssh://github.com/ballista-compute/ballista.git")
                        url.set("https://github.com/ballista-compute/ballista/")
                    }
                }

                from(components["kotlin"])
                artifact(sourcesJar)
                artifact(javadocJar)
            }
        }
    }

    signing {
        setRequired({
            (project.extra["isReleaseVersion"] as Boolean) && gradle.taskGraph.hasTask("publish")
        })
        sign(publishing.publications["mavenKotlin"])
    }

}

