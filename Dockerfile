FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7
COPY . .
RUN pip install -r requirements.txt
ENTRYPOINT ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
