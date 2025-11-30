FROM apache/airflow:2.7.1

# Copy the requirements file into the image
COPY requirements.txt /requirements.txt

# Install the dependencies
RUN pip install --no-cache-dir -r /requirements.txt