# Build: sudo docker build -t <project_name> .
# Run: sudo docker run -v $(pwd):/workspace/project --gpus all -it --rm <project_name>

FROM python:3.10

ENV PYTHONIOENCODING=UTF-8
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update \
    && apt install -y bash \
                   build-essential \
                   curl \
                   ca-certificates \
                   wget \
    && rm -rf /var/lib/apt/lists

# Create and activate a virtual environment
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"
ENV VIRTUAL_ENV="/venv"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"


# Set working directory
WORKDIR /workspaces/super-grpc

# Install Python dependencies
# Install Python dependencies
COPY dev-requirements.txt /tmp/
RUN /venv/bin/pip install --no-cache-dir -r /tmp/dev-requirements.txt \
    && rm /tmp/dev-requirements.txt


# Set PYTHONPATH
ENV PYTHONPATH=/workspaces/super-grpc

# Copy project files
# Copy specific folders
#COPY ./superdl /workspace/superdl

# Set PYTHONPATH
#ENV PYTHONPATH=/workspaces/super-grpc


CMD ["/bin/bash"]
