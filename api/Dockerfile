# Dockerfile pour Airflow + API FastAPI
FROM apache/airflow:2.7.1-python3.11

# Passer en root pour installer des packages si nécessaire
USER root

# Installer pip (déjà présent) et dépendances système si besoin
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copier les requirements Python depuis le dossier api
COPY api/requirements.txt /tmp/requirements.txt

# Installer les dépendances Python
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copier le code source API et plugins Airflow depuis la racine du projet
COPY api/main.py /opt/airflow/src/main.py
COPY src/ /opt/airflow/src/
COPY plugins/ /opt/airflow/plugins/

# Définir le répertoire de travail
WORKDIR /opt/airflow

# Repasser en airflow user
USER airflow

# Commande par défaut (peut être remplacée par docker-compose)
CMD ["bash"]