plugins {
    kotlin("jvm") version "1.3.50" apply false
    `maven-publish`
    id("org.datlowe.maven-publish-auth") version "2.0.2"
}

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
        jcenter()
    }
    group = "org.ballistacompute"
    version = "0.2.0"
}

subprojects {
    apply {
        plugin("org.jetbrains.kotlin.jvm")
        plugin("maven-publish")
        plugin("maven-publish-auth")
    }

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

    publishing {
        repositories {
            maven {
                val releasesRepoName = "sonatype"
                val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2")
                val snapshotsRepoName = "sonatype-snapshots"
                val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots")
                name = if (project.hasProperty("release")) releasesRepoName else snapshotsRepoName
                url = uri(if (project.hasProperty("release")) releasesRepoUrl else snapshotsRepoUrl)
                credentials {
                                 username = System.getenv("SONATYPE_USERNAME")
                                 password = System.getenv("SONATYPE_PASSWORD")
                             }
            }
        }
        publications {
            create<MavenPublication>("maven") {
                groupId = "org.ballistacompute"
                version = rootProject.version as String?

                pom {
                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    scm {
                        connection.set("scm:git:git://github.com/ballista-compute/ballista.git")
                        developerConnection.set("scm:git:ssh://github.com/ballista-compute/ballista.git")
                        url.set("https://github.com/ballista-compute/ballista/")
                    }
                }

                from(components["kotlin"])
            }
        }
    }

//    uploadArchives {
//        repositories {
//            mavenDeployer {
//                repository(url: "https://oss.sonatype.org/content/repositories/snapshots") {
//                authentication(userName: nexusUsername, password: nexusPassword)
//            }
//                snapshotRepository(url: "https://oss.sonatype.org/content/repositories/snapshots") {
//                authentication(userName: nexusUsername, password: nexusPassword)
//            }
//            }
//        }
//    }
}

