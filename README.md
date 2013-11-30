=================================
FH Münster - Big Data Engineering
=================================

Code für Übung Nummer 3
-----------------------

Das Repository enthält die MapReduce Beispiele aus der letzten Vorlesung.

Anstatt diese über Eclipse zu übersetzen, ist hier auf Maven umgestellt worden. Damit ist es einfacher die benötigten Bibliotheken einzubinden. Alles was lokal benötigt wird ist Java und Maven. Dieses kann über http://maven.apache.org/ heruntergeladen werden. Nach dem Entpacken steht in dem enthaltenen `bin` Verzeichnis das `mvn` Script zur Verfügung. Hier die Schritte im Einzelnen:

`$ wget http://mirror.derwebwolf.net/apache/maven/maven-3/3.1.1/binaries/apache-maven-3.1.1-bin.tar.gz`
`$ tar -zxvf apache-maven-3.1.1-bin.tar.gz`
`$ apache-maven-3.1.1/bin/mvn`

Am besten ist es das Verzeichnis, in dem Maven entpackt wurde, in der Shell Umgebung bekannt zu machen:

`$ EXPORT M2_HOME="<pfad>/apache-maven-3.1.1"`

Wenn Maven installiert ist, kann das Projekt wie folgt übersetzt werden:

`$ git clone https://github.com/larsgeorge/fh-muenster-bde-lesson-3.git`
`$ cd fh-muenster-bde-lesson-3`
`$ $M2_HOME/bin/mvn package`

Danach wie zuvor die JAR Datei aufrufen:

'$ hadoop jar target/h-muenster-bde-lesson-3.jar'

Viel Glück!

Lars George