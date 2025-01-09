plugins {
    id("java-library")
    id("scala")
    idea
    id("com.gradleup.shadow") version "8.3.0"
    checkstyle
    id("com.github.alisiikh.scalastyle") version "3.4.1"
    `maven-publish`
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks.shadowJar {
    isZip64 = true
}

tasks.javadoc {
    isFailOnError = true
}

scalastyle {
    config = file("${projectDir}/config/scalastyle_config.xml")
    verbose = true
    failOnWarning = false
}

repositories {
    mavenCentral()
}

idea {
    module {
        isDownloadJavadoc = true
        isDownloadSources = true
    }
}

dependencies {
    implementation("com.google.guava:guava:21.0")
    implementation("org.apache.commons:commons-math3:3.6.1")

    // Libraries for Spark
    implementation("org.scala-lang:scala-library:2.12.15")
    implementation("org.scala-lang:scala-reflect:2.12.15")
    implementation("org.scala-lang:scala-compiler:2.12.15")
    implementation("org.apache.spark:spark-core_2.12:3.5.1")
    implementation("org.apache.spark:spark-sql_2.12:3.5.1")
    implementation("org.apache.spark:spark-hive_2.12:3.5.1")
    implementation("org.apache.spark:spark-streaming_2.12:3.5.1")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.3.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.3.1")

    // Hadoop AWS library
    implementation("org.apache.hadoop:hadoop-aws:3.2.1")

    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_2.13:2.15.2")
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "1G"
}

tasks {
    register("defaultTasks") {
        dependsOn("clean", "build", "check", "javadoc", "jar")
    }
}
