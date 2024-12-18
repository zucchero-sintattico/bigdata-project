/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Scala application project to get you started.
 * For more details on building Java & JVM projects, please refer to https://docs.gradle.org/8.4/userguide/building_java_projects.html in the Gradle documentation.
 */

plugins {
    // Apply the scala Plugin to add support for Scala.
    scala

    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    // Use Scala 2.13 in our library project
    implementation("org.scala-lang:scala-library:2.13.11")

    // This dependency is used by the application.
    implementation("com.google.guava:guava:32.1.1-jre")

    // Use Scalatest for testing our library
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.scalatest:scalatest_2.13:3.2.16")
    testImplementation("org.scalatestplus:junit-4-13_2.13:3.2.2.0")

    // Need scala-xml at test runtime
    testRuntimeOnly("org.scala-lang.modules:scala-xml_2.13:1.2.0")
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    // Define the main class for the application.
    mainClass.set("bigdata.project.App")
}
