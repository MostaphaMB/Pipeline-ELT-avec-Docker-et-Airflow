# 🚀 Industrialisation du Pipeline ELT avec Docker et Airflow

## 📝 Présentation du Projet
Ce projet consiste à industrialiser un pipeline de données existant (basé sur l'API YouTube Data v3) en passant d'un script local à une architecture moderne et automatisée. L'objectif est de garantir la portabilité, la reproductibilité et l'orchestration du flux de données en utilisant **Docker** pour la conteneurisation et **Apache Airflow** pour l'ordonnancement des tâches.

## 🎯 Objectifs du Projet
* **Conteneurisation** : Isoler les services (Airflow, PostgreSQL, Python) via Docker pour assurer un environnement de développement stable.
* **Orchestration** : Concevoir un **DAG (Directed Acyclic Graph)** Airflow pour automatiser les étapes d'extraction, de chargement et de transformation.
* **Architecture ELT** : Implémenter une approche *Extract, Load, Transform* avec un stockage intermédiaire dans un Data Warehouse PostgreSQL.
* **Modélisation Staging/Core** : Organiser la base de données en couches distinctes pour séparer les données brutes des données transformées.
* **Maîtrise des Hooks** : Utiliser les Hooks Airflow pour interagir avec l'API YouTube et la base de données PostgreSQL.

## 🛠️ Stack Technique
* **Orchestrateur** : Apache Airflow.
* **Conteneurisation** : Docker & Docker Compose.
* **Base de Données** : PostgreSQL (Data Warehouse).
* **Langage** : Python (Ingestion API YouTube).
* **Client SQL** : DBeaver (pour la validation des données).

---

## 🏗️ Architecture du Pipeline

L'infrastructure est déployée via **Docker Compose** et s'articule autour des composants suivants :

1.  **Airflow Scheduler & Webserver** : Gestion et visualisation du cycle de vie du pipeline.
2.  **PostgreSQL (DW)** :
    * **Schéma Staging** : Réception des données brutes extraites de l'API YouTube.
    * **Schéma Core** : Stockage des données nettoyées, dédoublonnées et structurées pour l'analyse.
3.  **YouTube API v3** : Source de données externe pour l'ingestion des métriques vidéos.

---

## 🚀 Étapes de Réalisation

### 1. Environnement & Dockerisation 🐳
* Configuration du fichier `docker-compose.yaml` pour orchestrer les services Airflow et PostgreSQL.
* Création d'un `Dockerfile` personnalisé pour inclure les dépendances Python nécessaires.
* Initialisation de la base de données et des connexions Airflow (Connections & Variables).

### 2. Développement du DAG Airflow 🔄
* **Task 1 : Extraction** : Appel à l'API YouTube et récupération des métadonnées (vues, likes, titres).
* **Task 2 : Chargement (Staging)** : Insertion des données brutes dans la couche de staging de PostgreSQL.
* **Task 3 : Transformation (Core)** : Exécution de scripts SQL ou Python pour transformer les données et alimenter la couche finale.

### 3. Gestion et Monitoring 🛠️
* Configuration des **Retries** et des alertes en cas d'échec de tâche.
* Surveillance de l'exécution via l'interface web d'Airflow (Logs, Tree View, Graph View).
* Validation de l'intégrité des données dans la couche Core via des requêtes SQL.
