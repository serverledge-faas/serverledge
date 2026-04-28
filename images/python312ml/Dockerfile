FROM python:3.12-slim

ARG TARGETARCH


RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/* # remove useless cache afterwards for a lighter image

RUN pip install --no-cache-dir --upgrade pip

# multi-arch image differences
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        pip install --no-cache-dir tensorflow-cpu; \
    else \
        pip install --no-cache-dir tensorflow; \
    fi


RUN pip install --no-cache-dir \
    numpy \
    pandas \
    scikit-learn \
    scipy 

COPY executor.py /executor.py
WORKDIR /app

CMD ["python3", "/executor.py"]