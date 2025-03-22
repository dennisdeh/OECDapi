import subprocess
import os
import platform
from dotenv import load_dotenv


def initialise_redis_backend(path_dotenv_file):
    """
    Function to check whether the redis container is running or not.

    TODO
        Add automatic start of container if variable is set
    """
    print("Checking that redis is running... ", end="")
    # load redis port mapping from environment file
    out = load_dotenv(path_dotenv_file)
    if out:
        pass
    else:
        raise FileNotFoundError(".env file not found")

    # get environment variables
    redis_port = os.getenv("REDIS_PORT_HOST")
    redis_container_name = os.getenv("REDIS_CONT_NAME")
    if redis_port is None or redis_container_name is None:
        raise AssertionError(
            "Environment variables for the redis container are not found"
        )

    # assert that container is running
    if platform.system() == "Windows":
        try:
            s = subprocess.check_output("docker ps", shell=True)
        except subprocess.CalledProcessError:
            raise AssertionError(f"Docker is not running")
        if str(s).find(redis_container_name) == -1 and str(s).find(redis_port) == -1:
            raise AssertionError(f"The {redis_container_name} container is not running")
    elif platform.system() == "Linux":
        # TODO implement check later
        pass
    else:
        raise NotImplementedError(f"Unsupported platform: {platform.system()}")

    print("Success!")
