# Use the Python 3.11 image with useful data libraries
FROM mcr.microsoft.com/devcontainers/python:3.11-bullseye

# Set working directory
WORKDIR /workspace

# Install ODBC libraries and SQL Server ODBC Driver
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    libpq-dev \
    libsqlite3-dev \
    curl \
    gnupg \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Copy project files into the container
COPY . .

# Install Python dependencies from requirements.txt if it exists
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Install additional Python dependencies if not included in requirements.txt
RUN pip install \
    notebook==7.0.7 \
    pandas==2.0.3 \
    pyarrow==15.0.0 \
    seaborn==0.13.2 \
    sweetviz==2.3.1 \
    pyodbc==5.2.0 \
    SQLAlchemy==2.0.36 

# Set default command
CMD ["bash"]

