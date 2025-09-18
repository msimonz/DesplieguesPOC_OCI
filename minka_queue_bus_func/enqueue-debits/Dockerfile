FROM fnproject/python:3.11

WORKDIR /function

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# Forzar el entrypoint del FDK
ENTRYPOINT ["fdk", "/function/func.py"]
CMD ["handler"]