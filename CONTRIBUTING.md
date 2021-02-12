# Wallet

## Backend Requirements

* [Docker](https://www.docker.com/).
* [Docker Compose](https://docs.docker.com/compose/install/).


## Backend local development

* Start the stack with Docker Compose:

```bash
docker-compose up -d
```

* Now you can open your browser and interact with these URLs:


Backend, JSON based web API based on OpenAPI: http://localhost/api/

Automatic interactive documentation with Swagger UI (from the OpenAPI backend): http://localhost/docs

**Note**: The first time you start your stack, it might take a minute for it to be ready. While the backend waits for the database to be ready and configures everything. You can check the logs to monitor it.

To check the logs, run:

```bash
docker-compose logs
```

To check the logs of a specific service, add the name of the service, e.g.:

```bash
docker-compose logs wallet
```

## Backend local development, additional details

### General workflow

By default, the dependencies are managed with [PIP](https://pip.pypa.io/en/stable/installing/), go there and install it.

You can install all the dependencies with:

```console
$ pip install -r requirements/requirements.txt
```

### Docker Compose Override

During development, you can change Docker Compose settings that will only affect the local development environment, in the file `docker-compose.override.yml`.

The changes to that file only affect the local development environment, not the production environment. So, you can add "temporary" changes that help the development workflow.


```console
$ docker-compose up -d
```

There is also a commented out `command` override, you can uncomment it and comment the default one. It makes the backend container run a process that does "nothing", but keeps the container alive. That allows you to get inside your running container and execute commands inside, for example a Python interpreter to test installed dependencies, or start the development server that reloads when it detects changes, or start a Jupyter Notebook session.

To get inside the container with a `bash` session you can start the stack with:

```console
$ docker-compose up -d
```

and then `exec` inside the running container:

```console
$ docker-compose exec wallet bash
```

You should see an output like:

```console
root@7f2607af31c3:/app#
```

that means that you are in a `bash` session inside your container, as a `root` user, under the `/app` directory.

### Backend tests

To test the backend run:

```console
$ pytest .
```

If you use GitLab CI the tests will run automatically.

#### Build with dev packages

```Bash
PIP_PACKAGES_FILE=requirements.dev.txt docker-compose build
```

#### Local tests

You can rerun the test on live code:

```Bash
docker exec wallet_maintenance pytest
```

#### Test Coverage

Because the test scripts forward arguments to `pytest`, you can enable test coverage HTML report generation by passing `--cov-report=html`.

To run the local tests with coverage HTML reports:

```Bash
pytest . --cov-report=html
```

To run the tests in a running stack with coverage HTML reports:

```bash
docker exec wallet_maintenance pytest . --cov-report=html
```

