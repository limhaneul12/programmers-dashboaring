FROM apache/airflow:2.6.1

# Switch to the Airflow user
USER airflow

# Copy requirements.txt
COPY requirements.txt ./requirements.txt

# Install Python dependencies as the airflow user
RUN pip install --user --no-cache-dir -r requirements.txt