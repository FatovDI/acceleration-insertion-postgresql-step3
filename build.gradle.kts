import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	id("org.springframework.boot") version "2.6.4"
	id("io.spring.dependency-management") version "1.1.0"
	id("org.jetbrains.kotlin.kapt") version "1.8.10"
	kotlin("jvm") version "1.8.21"
	kotlin("plugin.spring") version "1.8.21"
}

group = "com.example"
java.sourceCompatibility = JavaVersion.VERSION_17

repositories {
	mavenCentral()
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter")
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-starter-data-jpa")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.1")
	implementation("jakarta.persistence:jakarta.persistence-api")
	implementation("org.flywaydb:flyway-core")
	implementation("org.postgresql:postgresql:42.6.0")
	implementation("org.hibernate:hibernate-jpamodelgen")
	implementation("com.fasterxml.uuid:java-uuid-generator:5.1.0")

	kapt("org.hibernate:hibernate-jpamodelgen")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.testcontainers:postgresql:1.17.6")
	testImplementation("org.testcontainers:junit-jupiter:1.17.6")
}

allOpen {
	annotation("javax.persistence.Entity")
	annotation("javax.persistence.MappedSuperclass")
	annotation("javax.persistence.Embeddable")
}

tasks.withType<KotlinCompile> {
	kotlinOptions {
		freeCompilerArgs = listOf("-Xjsr305=strict")
		jvmTarget = "17"
	}
}

tasks.withType<Test> {
	useJUnitPlatform{
		excludeTags("stressTest")
	}
}
