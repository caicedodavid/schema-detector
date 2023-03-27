  FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

  ARG WORKDIR=/dataflow/template
  RUN mkdir -p ${WORKDIR}
  WORKDIR ${WORKDIR}

  COPY requirements.txt .
  COPY main.py .
  COPY libs libs/

  ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
  ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"

  RUN pip install apache-beam[gcp]
  RUN pip install -U -r ./requirements.txt