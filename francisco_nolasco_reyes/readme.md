# The Api Cat with Kafka

Esta aplicación recupera información del api-cat, la transforma y guarda en base de datos.

## Descripción

La idea final es obtener la información relevante del gato, para proponerlo en un sitio web y sugerir especies para adopción.

## Empezando

### Dependencias

* Hay que crear un enviroment dentro de /francisco_nolasco_reyes, ejemplo: cat_api_env
```
python -m venv cat_api_env
```
* También hay que activarlo
```
.\cat_api_env\Scripts\activate
```
* Las dependencias se pueden encontrar dentro del archivo requirements.txt, las cuales deben ser instaladas
```
pip install -r requirements.txt
```

### Ejecutando el programa

Hay que tener listo y corriendo docker con las imágenes de:

* Kafka
* MySQL

Considerar tener dada de una base de datos: apicat, también la tabla en la base de datos con la siguiente estructura:

```
-- apicat.api_data definition

CREATE TABLE `api_data` (
  `id` int NOT NULL AUTO_INCREMENT,
  `data_obtained` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=148 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

Después:

* Primero correr el producer

```
python .\producer_functions.py
```

* Luego correr el consumer

```
python .\consumer_functions.py
```

## Authors

Contributors names and contact info

Francisco Nolasco
fnolasco@apexsystems.com

## Historial de versiones

* 0.1
    * Funcionamiento básico

## Licencia

This project is licensed under the Apex Systems License