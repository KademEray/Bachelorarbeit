FROM postgres:17.5

# Setzt Umgebungsvariablen für den Standard-Datenbanknamen, Benutzer und Passwort
ENV POSTGRES_DB=testdb \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=pass \
    LANG="en_US.utf8"

# Kopiert die benutzerdefinierte PostgreSQL-Konfiguration in das automatische Initialisierungsverzeichnis.
# Alle .conf-Dateien im Verzeichnis /docker-entrypoint-initdb.d/ werden beim ersten Start automatisch angewendet.
COPY postgres-tuning.conf /docker-entrypoint-initdb.d/
