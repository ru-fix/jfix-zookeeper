
object Vers {
    //Plugins
    const val dokkav = "0.9.18"
    const val gradle_release_plugin = "1.3.9"
    const val asciidoctor = "1.5.9.2"

    //Dependencies
    val kotlin = "1.3.41"
    val dokka = "0.9.18"
    val jfix_stdlib = "1.0.22"
    val lombok = "1.18.4"
    val jackson = "2.9.6"
    val jackson_kotlin = "2.9.6"
    val apache_curator = "2.10.0"
    val logback = "1.1.11"
    val commons_io = "2.4"
    val junit4 = "4.12"
}

object Libs {
    //Plugins
    const val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    const val dokka_gradle_plugin =  "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"
    const val asciidoctor = "org.asciidoctor:asciidoctor-gradle-plugin:${Vers.asciidoctor}"
    const val nexus_staging_plugin = "io.codearte.nexus-staging"
    const val nexus_publish_plugin = "de.marcphilipp.nexus-publish"

    //Dependencies
    val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"

    val jfix_stdlib_socket = "ru.fix:jfix-stdlib-socket:${Vers.jfix_stdlib}"
    val lombok = "org.projectlombok:lombok:${Vers.lombok}"
    val jackson_annotations = "com.fasterxml.jackson.core:jackson-annotations:${Vers.jackson}"
    val jackson_datatype_jsr310 = "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${Vers.jackson}"
    val jackson_kotlin_module = "com.fasterxml.jackson.module:jackson-module-kotlin:${Vers.jackson_kotlin}"
    val apache_curator = "org.apache.curator:curator-recipes:${Vers.apache_curator}"
    val apache_curator_test = "org.apache.curator:curator-test:${Vers.apache_curator}"
    val junit4 = "junit:junit:${Vers.junit4}"
    val logback = "ch.qos.logback:logback-classic:${Vers.logback}"
    val commons_io = "commons-io:commons-io:${Vers.commons_io}"
}