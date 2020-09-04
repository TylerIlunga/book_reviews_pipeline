FROM:python3.7

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

RUN python app.py
