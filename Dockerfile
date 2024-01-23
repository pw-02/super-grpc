# Use Ubuntu minimal image
FROM ubuntu:22.04

# Set environment variables

ENV PATH /opt/conda/bin:$PATH
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/usr/local/lib"
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/opt/conda/lib"
ENV PYTHONIOENCODING=UTF-8
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
ENV CONDA_AUTO_UPDATE_CONDA=false

# Set the working directory
WORKDIR /workspace

# Install required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        #build-essential \
        git \
        curl \
        ca-certificates \
        wget \
    && rm -rf /var/lib/apt/lists/*


# # Copy the entrypoint script into the container
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Install Miniconda
ADD https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh miniconda3.sh
RUN /bin/bash miniconda3.sh -b -p /opt/conda && \
    rm miniconda3.sh && \
    /opt/conda/bin/conda install -y -c anaconda python=3.10 && \
    /opt/conda/bin/conda clean -ya

# Install requirements
COPY requirements.txt /tmp/
RUN /opt/conda/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt


ENV PYTHONPATH=/workspace/super-grpc

# # Copy the source code for the server into the container
# COPY server /supergprc/server
# COPY proto /supergprc/proto

# Expose the necessary port
EXPOSE 50051

# Cleanup
RUN apt-get clean && \
    apt-get autoremove -y && \
    apt-get autoclean -y && \
    rm -rf /var/lib/apt/lists/*

# Set the default command
#CMD ["/bin/bash"]
# Specify the command to run on container startup
CMD ["/usr/local/bin/entrypoint.sh"]
#CMD ["aws", "configure"]
#CMD ["python", "server.py"]