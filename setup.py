from setuptools import setup, find_packages

setup(
    name='super-grpc-client',
    version='0.1',
    packages=find_packages(include=['client', 'protos']),
    install_requires=[
        'grpcio',
        # Add any other dependencies here
    ],
)