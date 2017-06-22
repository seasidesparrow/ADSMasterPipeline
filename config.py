CELERY_DEFAULT_EXCHANGE = 'master_pipeline'
CELERY_DEFAULT_EXCHANGE_TYPE = "topic"

# Connection to the database where we save orcid-claims (this database
# serves as a running log of claims and storage of author-related
# information). It is not consumed by others (ie. we 'push' results) 
# SQLALCHEMY_URL = 'postgres://docker:docker@localhost:6432/docker'
SQLALCHEMY_URL = 'sqlite:///'
SQLALCHEMY_ECHO = False


# Configuration of the pipeline; if you start 'vagrant up rabbitmq' 
# container, the port is localhost:8072 - but for production, you 
# want to point to the ADSImport pipeline 
RABBITMQ_URL = 'amqp://guest:guest@localhost:6672/?' \
               'socket_timeout=10&backpressure_detection=t'
               

# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'