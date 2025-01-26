# test_mysql.py
import pymysql
import ssl

try:
    connection = pymysql.connect(
        host='europe-priva85.privatednsorg.com',
        port=3306,  # Cambiado a puerto estándar MySQL
        user='laboralpensiones_agent',
        password='fzb^IOr!1^i9wTo6',
        database='laboralp_wp925',
        ssl={
            'ssl': {
                'ca': None,  # Ruta al certificado CA si es necesario
                'check_hostname': False,
                'verify_mode': ssl.CERT_NONE
            }
        }
    )
    print("¡Conexión exitosa!")
    
    # Prueba simple de consulta
    with connection.cursor() as cursor:
        cursor.execute("SELECT `comment_content` FROM `wpta_comments` LIMIT 1")
        result = cursor.fetchone()
        print(f"Resultado de prueba: {result}")
        
except Exception as e:
    print(f"Error de conexión: {e}")
finally:
    if 'connection' in locals():
        connection.close()