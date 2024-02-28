import os
import xml.etree.ElementTree as ET
import sqlite3

# Función para extraer el texto completo de un elemento, incluyendo subelementos
def extraer_texto_completo(elemento, tag):
    sub_elemento = elemento.find(tag)
    if sub_elemento is not None:
        return ''.join(sub_elemento.itertext()).strip()
    return None

# Función para extraer una sección por palabras clave, incluyendo todo el texto de subelementos
def extraer_seccion_completa(elemento, palabras_clave):
    for seccion in elemento.findall('.//sec'):
        titulo_seccion = ''.join(seccion.find('.//title').itertext()).strip().lower()
        if any(palabra_clave.lower() in titulo_seccion for palabra_clave in palabras_clave):
            return ''.join(seccion.itertext()).strip()
    return None

# Ruta a la carpeta que contiene los archivos XML
ruta_carpeta = r'/home/isabella_gallego/OneDrive/Documentos/LabBCES/IC'

# Conectar a la base de datos SQLite
conn = sqlite3.connect('pubmed_articles.db')
cursor = conn.cursor()

# Lista de nombres de revistas de interés
# Journal of Translational Medicine
# Health and Quality of Life Outcomes
revistas_de_interes = ['PLoS Biology', 'Journal of Translational Medicine']

for archivo in os.listdir(ruta_carpeta):
    if archivo.endswith('.xml'):
        print("archivos encontrados: ",archivo) #encontrando problemas ...
        ruta_archivo = os.path.join(ruta_carpeta, archivo)
        tree = ET.parse(ruta_archivo)
        root = tree.getroot()

        nombre_revista = extraer_texto_completo(root, './/journal-title')

        # Verificar si el artículo pertenece a una revista de interés
        for revista in revistas_de_interes:
            if nombre_revista.strip().lower() in revista.lower():
                # Extracción de los campos usando las funciones modificadas
                print("revista encontrada:", nombre_revista)
                pmid = extraer_texto_completo(root, './/article-id[@pub-id-type="pmid"]')
                titulo = extraer_texto_completo(root, './/article-title')
                doi = extraer_texto_completo(root, './/article-id[@pub-id-type="doi"]')
                nombre_revista = extraer_texto_completo(root, './/journal-title')
                año_publicacion = extraer_texto_completo(root, './/pub-date/year')
                primer_autor = extraer_texto_completo(root, './/contrib-group/contrib[@contrib-type="author"]/name')

                # Palabras clave para las secciones de métodos y resultados
                palabras_clave_metodos = ['methods', 'methodology', 'materials']
                palabras_clave_resultados = ['results', 'findings']

                # Extracción de métodos y resultados
                metodos = extraer_seccion_completa(root, palabras_clave_metodos)
                resultados = extraer_seccion_completa(root, palabras_clave_resultados)

                # Estructuración de los datos extraídos
                datos_articulo = {
                    "PMID": pmid,
                    "Título": titulo,
                    "DOI": doi,
                    "Nombre de la Revista": nombre_revista,
                    "Año de Publicación": año_publicacion,
                    "Primer Autor": primer_autor,
                    "Métodos": metodos,
                    "Resultados": resultados
                    }

                # Conectar a la base de datos SQLite (esto creará la base de datos si no existe)
                conn = sqlite3.connect('pubmed_articles.db')
                cursor = conn.cursor()

                # Crear tabla si no existe
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS articles (
                        pmid TEXT PRIMARY KEY,
                        title TEXT,
                        year TEXT,
                        doi TEXT,
                        journal_name TEXT,
                        first_author TEXT,
                        abstract TEXT,
                        methods TEXT,
                        results TEXT
                        )
                ''')

                # Preparar la sentencia SQL para insertar datos
                sql = '''
                   INSERT INTO articles (pmid, title, year, doi, journal_name, first_author, abstract, methods, results)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''

                # Datos a insertar
                abstract = extraer_texto_completo(root, './/abstract')
                datos_articulo = {
                    "PMID": pmid,
                    "Título": titulo,
                    "Año de Publicación": año_publicacion,
                    "DOI": doi,
                    "Nombre de la Revista": nombre_revista,
                    "Primer Autor": primer_autor,
                    "Resumen": abstract,
                    "Métodos": metodos,
                    "Resultados": resultados
                }

                datos_a_insertar = (
                    datos_articulo['PMID'], 
                    datos_articulo['Título'], 
                    datos_articulo['Año de Publicación'], 
                    datos_articulo['DOI'], 
                    datos_articulo['Nombre de la Revista'], 
                    datos_articulo['Primer Autor'], 
                    datos_articulo['Resumen'], 
                    datos_articulo['Métodos'], 
                    datos_articulo['Resultados']
                )

                # Ejecutar la sentencia SQL 
                try:
                    cursor.execute(sql, datos_a_insertar)
                    conn.commit()
                    print("Datos insertados con éxito.")
                except sqlite3.IntegrityError:
                    print("Error: El artículo con este PMID ya existe en la base de datos.")
                except Exception as e:
                    print("Error al insertar los datos:", e)

                # Cerrar la conexión
                conn.close()
                print('------------')
