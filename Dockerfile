# Settings -> Developer Settings -> Personal Access Tokens
# testing_pacakges token: 5f09ff00161da52128e717b8c217b59270683052

# Authenticate && Login:
# cat ~/GH_TOKEN.txt | docker login docker.pkg.github.com -u TylerIlunga --password-stdin

# Tag:
# docker tag IMAGE_ID docker.pkg.github.com/tylerilunga/repository-name/IMAGE_NAME:VERSION
# docker build -t docker.pkg.github.com/tylerilunga/book_reviews_pipeline/app:v0.0.0 .
# docker build -t ghcr.io/OWNER/IMAGE-NAME .

# Publish
# docker push docker.pkg.github.com/tylerilunga/repository-name/IMAGE_NAME:VERSION
# docker push docker.pkg.github.com/tylerilunga/book_reviews_pipeline/app:v0.0.0

FROM python:3.8-slim-buster

ENV VIRTUAL_ENV=/opt/venv

RUN python3 -m venv $VIRTUAL_ENV

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "app.py"]
