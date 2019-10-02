
object Vers {
    //Plugins
    const val dokkav = "0.9.18"
    const val gradle_release_plugin = "1.3.9"
    const val asciidoctor = "1.5.9.2"

    //Dependencies
    const val kotlin = "1.3.50"
    const val jfix_stdlib = "1.0.59"
    const val lombok = "1.18.4"
    const val jackson = "2.9.6"
    const val jackson_kotlin = "2.9.6"
    const val apache_curator = "4.2.0"
    const val logback = "1.1.11"
    const val commons_io = "2.4"
    const val junit4 = "4.12"
}

object Libs {
    //Plugins
    const val gradle_release_plugin = "ru.fix:gradle-release-plugin:${Vers.gradle_release_plugin}"
    const val dokka_gradle_plugin =  "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"
    const val asciidoctor = "org.asciidoctor:asciidoctor-gradle-plugin:${Vers.asciidoctor}"
    const val nexus_staging_plugin = "io.codearte.nexus-staging"
    const val nexus_publish_plugin = "de.marcphilipp.nexus-publish"

    //Dependencies
    const val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    const val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    const val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"

    const val jfix_stdlib_socket = "ru.fix:jfix-stdlib-socket:${Vers.jfix_stdlib}"
    const val lombok = "org.projectlombok:lombok:${Vers.lombok}"
    const val jackson_annotations = "com.fasterxml.jackson.core:jackson-annotations:${Vers.jackson}"
    const val jackson_datatype_jsr310 = "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${Vers.jackson}"
    const val jackson_kotlin_module = "com.fasterxml.jackson.module:jackson-module-kotlin:${Vers.jackson_kotlin}"
    const val apache_curator = "org.apache.curator:curator-recipes:${Vers.apache_curator}"
    const val apache_curator_test = "org.apache.curator:curator-test:${Vers.apache_curator}"
    const val junit4 = "junit:junit:${Vers.junit4}"
    const val logback = "ch.qos.logback:logback-classic:${Vers.logback}"
    const val commons_io = "commons-io:commons-io:${Vers.commons_io}"
}
