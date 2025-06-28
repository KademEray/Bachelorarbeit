# Verwendet das offizielle Neo4j-Image in Version 5.26.6 als Basis für den Container
FROM neo4j:5.26.6

# Setzt die Zugangsdaten für die Neo4j-Datenbank (Benutzername/Passwort) über Umgebungsvariablen.
ENV NEO4J_AUTH=neo4j/superpassword55

# Kopiert eine benutzerdefinierte Konfigurationsdatei (neo4j.conf) in das vorgesehene Verzeichnis des Containers.
COPY --chown=neo4j:neo4j neo4j.conf /var/lib/neo4j/conf/neo4j.conf
