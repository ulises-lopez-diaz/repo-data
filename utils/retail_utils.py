import os
import kaggle

def realizar_autenticacion_en_kaggle():
    
    # Se utiliza el api key proporcioando para kaggle
    kaggle.api.authenticate()


def verificar_directorio(nombre_directorio="bronze"):
    # Obtener el directorio actual
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    
    # Obtener el directorio padre
    directorio_padre = os.path.dirname(directorio_actual)
    
    # Construir las rutas de los directorios
    ruta_data = os.path.join(directorio_padre, 'data')
    ruta_bronze = os.path.join(ruta_data, 'bronze')
    
    # Verificar si la carpeta 'data' existe
    if not os.path.isdir(ruta_data):
        print(f"La carpeta 'data' no existe. Creándola en: {ruta_data}")
        os.makedirs(ruta_data)  # Crear la carpeta 'data'
    
    # Verificar si la carpeta 'bronze' existe dentro de 'data'
    if not os.path.isdir(ruta_bronze):
        print(f"La carpeta 'bronze' no existe dentro de 'data'. Creándola en: {ruta_bronze}")
        os.makedirs(ruta_bronze)  # Crear la carpeta 'bronze'
    
    # Confirmación final
    print(f"Carpetas verificadas y creadas si era necesario:")
    print(f"- data: {'Existe' if os.path.isdir(ruta_data) else 'No existe'}")
    print(f"- bronze: {'Existe' if os.path.isdir(ruta_bronze) else 'No existe'}")