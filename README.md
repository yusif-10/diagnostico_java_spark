# Examen Diagnostico JAVA to Big Data v2
**cuentas con 24 horas para resolver el ejercicio**


## Instrucciones

1. Realizar un fork de este repositorio a tu cuenta de github.
2. Crear una rama que por nombre lleve tus iniciales a partir de la rama solution.
3. Realizar los ejercicios solicitados abajo.
4. Enviar por correo electronico la notificación de finalización y el link al repositorio de solución.

**_No debes hacer PULL REQUEST_**

## ¿Qué evaluaremos?

* Resuelva con el API de SparkSQL el ejercicio planteado.
* El uso de sentencias SQL queda estrictamente prohibido.
* El uso de cadenas en las clases que implementan la lógica de solución están muy mal vistos por nuestra area de QA, sea
  cuidadoso.
* Amamos las pruebas de calidad, es necesario que el método 4 cuente con una prueba unitaria 
    que valide que se han filtrado correctamente los jugadores.
* Modularice sú código lo suficiente de tal forma que cada método haga una sola cosa.
* Hemos soñado con poder leer las rutas de entrada y salida desde un archivo de configuración, sería increible tener
  uno! (crear un archivo properties)

## Ejercicio

1. La tabla de salida debe contener las siguientes columnas:
   `short_name, long_name, age, height_cm, weight_kg, nationality, club_name, overall, potential, team_position`
   
2. Agregar una columna `age_range` qué responderá a la siguiente regla:
    * **A** si el jugador es menor de 23 años
    * **B** si el jugador es menor de 27 años
    * **C** si el jugador es menor de 32 años
    * **D** si el jugador tiene 32 años o más
    
    ***pista** la función `org.apache.spark.sql.functions.when` podría ser de gran utilidad*
3. Agregaremos una columna `rank_by_nationality_position` con la siguiente regla:
    Para cada país (`nationality`) y posición(`team_position`) debemos ordenar a los jugadores por la columna `overall`
    de forma descendente y colocarles un número generado por la función `row_number`
     
   ***pista** para resolver este ejercicio, mire el método de ejemplo exampleWindowFunction incluido en el código.*
4. Agregaremos una columna `potential_vs_overall` cuyo valor estará definido por la siguiente regla:
   
   *`potential_vs_overall` = `potential`/`overall`*
    
5. Filtraremos de acuerdo a las columnas `age_range` y `rank_by_nationality_position` con las siguientes condiciones:
    * Si `rank_by_nationality_position` es menor a **3**
    * Si `age_range` es **B** o **C** y `potential_vs_overall` es superior a **1.15**
    * Si `age_range` es **A** y `potential_vs_overall` es superior a **1.25**
    * Si `age_range` es **D** y `rank_by_nationality_position` es menor a **5**

6. Por favor escriba la tabla resultante de los pasos anteriores particionada por la columna `nationality`, la salida
   debe estar escrita en formato **parquet** y debe usarse el método `coalese(1)`
   para obtener solo un archivo por partición.

¡Buena suerte!
