# Usa la imagen oficial de Python como base
FROM python:3.11.9-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar las dependencias del sistema operativo (crucial para compilar librerías Python)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    # CRUCIAL para Telethon/cryptography
    libffi-dev \
    # Limpiar caché para reducir el tamaño de la imagen
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements.txt e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código de la aplicación
COPY . .

# Crear el directorio de descargas si no existe y asegurar permisos
RUN mkdir -p /app/downloads

# Define el comando que se ejecutará al iniciar el contenedor. 
# Esto reemplaza el comando por defecto 'python3' que estaba fallando.
CMD ["gunicorn", "main:app", "--bind", "0.0.0.0:8080"]
