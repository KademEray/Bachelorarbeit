FROM postgres:17.5

ENV POSTGRES_DB=testdb \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=pass \
    LANG="en_US.utf8"

# Konfig-Snippet in das Init-Verzeichnis kopieren.
# Der offizielle Entry-Point liest *.conf im Unterordner automatisch ein.
COPY postgres-tuning.conf /docker-entrypoint-initdb.d/
