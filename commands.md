# Big Data Engineering - FH Münster - Projekt

[![Build Status](https://travis-ci.com/kutzilla/bde-project.svg?token=sVFsn6MbRsFLvenMx9sG&branch=master)](https://travis-ci.com/kutzilla/bde-project)

###Deployment

###Download Apache Tomcat 8 und entpacken
http://apache.mirror.iphh.net/tomcat/tomcat-8/v8.0.32/bin/apache-tomcat-8.0.32.tar.gz

Kopieren nach /opt/apache-tomcat-8.0.32

###Apache Port auf 8090 ändern:
###Bearbeiten der Konfigurationsdatei: /opt/apache-tomcat-8.0.32/conf/server.xml
    ...
    <Connector port="8090" protocol="HTTP/1.1"
                   connectionTimeout="20000"
                   redirectPort="8443" />
        <!-- A "Connector" using the shared thread pool-->
        <!--
        <Connector executor="tomcatThreadPool"
                   port="8090" protocol="HTTP/1.1"
                   connectionTimeout="20000"
                   redirectPort="8443" />
        -->
    ...


Kopieren    github-data-RestHbaseInterface/target/global-github-contributions.war   nach    /opt/apache-tomcat-8.0.32/webapps/
Kopieren    github-global-presentation/target/github-global-presentation.war    nach    /opt/apache-tomcat-8.0.32/webapps/

###Starten des Tomcats
sudo bash /bin/catalina.sh start


###Webseite erreichbar unter:
http://localhost:8090/global-github-contributions/rest/json/github/getAllCommitsOfAllCountries