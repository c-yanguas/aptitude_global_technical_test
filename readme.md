# Pasos para reproducir la prueba
1. Crear el entorno virtual con las dependencias definidas en requirements.txt
   1. Crear el entorno virtual: ```python -m venv project_env```
   2. Activar el entorno virtual
      1. Windows: ```project_env\Scripts\activate.bat```.
      2. Linux/Mac: ```source project_env/Scripts/activate```
   3. Instalar las dependencias en el entorno virtual: ```pip install -r requirements.txt```
2. Seleccionar el kernel del venv creado para este proyecto (project_env)
3. Ir al notebook facilitado src/dev.ipynb y ejecutar hasta la parte que dice test debugging. El razonamiento se puede ir viendo en las celdas de markdown dejadas


# Pasos para reproducir los tests
1. Activar el venv en el terminal:
   1.  Windows: ```project_env\Scripts\activate.bat```
   2.  Linux/Mac: ```source project_env/Scripts/activate```
2.  Ejecutar en la terminal posicionado en prueba_tecnica_cyd ```pytest```
3.  El resultado obtenido debería ser similar al ofrecido en la carpeta test "summary_pytest.txt"


# Otras aclaraciones
* No soy experto ni tengo mucha experiencia haciendo tests, los tests que requieren de mock (por ejemplo para llamadas a URLs) me cuestan bastante
* Me he apoyado en chatGPT para las siguientes acciones:
  * Generacion de docstrings tanto en funciones como en la clase
  * Generacion de tests
