# Dockerfile para app.py (productor)
FROM python:3.11.0

# Instalar dependencias del sistema necesarias para pyodbc
RUN apt-get update && \
    apt-get install -y \
    unixodbc \
    unixodbc-dev \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar el controlador ODBC de Microsoft para SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17

WORKDIR /app

COPY app.py .

RUN pip install pika pyodbc

CMD ["python", "app.py"]
