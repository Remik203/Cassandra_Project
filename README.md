# Libertyn - Cassandra Twitter Clone

## Opis projektu

Libertyn to aplikacja społecznościowa inspirowana platformą Twitter, zbudowana w oparciu o bazę danych Apache Cassandra. Projekt implementuje hybrydowy model dostarczania postów, który łączy wydajność modelu "Fan-out on Write" ze skalowalnością modelu "Fan-out on Read".

## Architektura

### Model danych
Aplikacja wykorzystuje cztery główne tabele w bazie danych Cassandra:

- **`moje_posty`** - tabela przechowująca posty użytkowników
- **`moja_os_czasu`** - timeline użytkowników (model Fan-out on Write)
- **`kto_mnie_obserwuje`** - lista obserwujących użytkownika (dla modelu Push)
- **`kogo_obserwuje`** - lista obserwowanych przez użytkownika (dla modelu Pull)

### Hybrydowy model dostarczania postów

#### Fan-out on Write (Push) - dla zwykłych użytkowników
- Posty są natychmiastowo dostarczane do timeline'ów wszystkich obserwujących
- Zapewnia szybki odczyt osi czasu
- Stosowany dla użytkowników posiadających maksymalnie 5000 obserwujących

#### Fan-out on Read (Pull) - dla celebrytów
- Posty są pobierane w czasie rzeczywistym podczas odczytu timeline'a
- Pozwala oszczędzać miejsce w bazie danych
- Stosowany dla użytkowników posiadających ponad 5000 obserwujących

## Funkcjonalności

- Publikowanie postów przez użytkowników
- System obserwowania innych użytkowników
- Wyświetlanie spersonalizowanej osi czasu (timeline)
- Przeglądanie profili użytkowników
- Automatyczne wykrywanie statusu celebryty
- Moduł testowania wydajności

## Instalacja

### Wymagania systemowe
- Python w wersji 3.11 lub nowszej
- Apache Cassandra w wersji 4.0 lub nowszej
- Biblioteki: cassandra-driver, PySimpleGUI

### Instrukcja instalacji

1. **Sklonowanie repozytorium**
   ```bash
   git clone https://github.com/Remik203/Cassandra_Project.git
   cd Cassandra_Project
   ```

2. **Konfiguracja środowiska wirtualnego**
   ```bash
   python3 -m venv .
   source bin/activate
   ```

3. **Instalacja wymaganych bibliotek**
   ```bash
   pip install -r requirements.txt
   ```

4. **Konfiguracja bazy danych Cassandra**
   ```sql
   CREATE KEYSPACE libertyn WITH replication = {
     'class': 'SimpleStrategy', 
     'replication_factor': '3'
   };
   
   USE libertyn;
   ```

5. **Utworzenie struktury tabel**

## Schemat bazy danych

```sql
-- Tabela postów użytkowników
CREATE TABLE moje_posty (
    username text,
    post_id timeuuid,
    content text,
    PRIMARY KEY (username, post_id)
) WITH CLUSTERING ORDER BY (post_id DESC);

-- Tabela osi czasu użytkowników
CREATE TABLE moja_os_czasu (
    username text,
    post_id timeuuid,
    author_username text,
    content text,
    PRIMARY KEY (username, post_id)
) WITH CLUSTERING ORDER BY (post_id DESC);

-- Tabela obserwujących użytkownika (dla modelu Push)
CREATE TABLE kto_mnie_obserwuje (
    username text,
    follower_username text,
    PRIMARY KEY (username, follower_username)
);

-- Tabela obserwowanych przez użytkownika (dla modelu Pull)
CREATE TABLE kogo_obserwuje (
    username text,
    following_username text,
    is_celebrity boolean,
    PRIMARY KEY (username, following_username)
);

-- Indeks dla zwiększenia wydajności zapytań
CREATE INDEX ON kogo_obserwuje (following_username);
```

## Uruchomienie aplikacji

1. **Uruchomienie bazy danych Cassandra**
   ```bash
   # Przy użyciu Docker
   cd Docker_conf
   docker compose up -d
   
   # Lub uruchomienie lokalne
   cassandra -f
   ```

2. **Uruchomienie aplikacji głównej**
   ```bash
   python3 main.py
   ```