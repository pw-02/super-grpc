FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt ./
RUN python3.9 -m pip install -r requirements.txt -t .

COPY app.py ./
# Copy NLTK data
# COPY nltk_data ${LAMBDA_TASK_ROOT}/nltk_data

# # Set environment variable for NLTK data
# ENV NLTK_DATA=${LAMBDA_TASK_ROOT}/nltk_data
# Copy NLTK data
COPY nltk_data /var/task/nltk_data
COPY gpt2-tokenizer /var/task/gpt2-tokenizer/
# Set environment variable for NLTK data
ENV NLTK_DATA=/var/task/nltk_data

#COPY model /opt/ml/model
CMD ["app.lambda_handler"]