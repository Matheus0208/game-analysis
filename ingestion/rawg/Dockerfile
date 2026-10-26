FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install --no-cache-dir -r requirements.txt

COPY app.py config.json ${LAMBDA_TASK_ROOT}/

CMD ["app.lambda_handler"]