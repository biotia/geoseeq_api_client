"""Utils to get token from config"""
from os import makedirs, environ
from os.path import join, exists
import json
from geoseeq.knex import DEFAULT_ENDPOINT

CONFIG_FOLDER = environ.get("XDG_CONFIG_HOME", join(environ["HOME"], ".config"))
CONFIG_DIR = environ.get("GEOSEEQ_CONFIG_DIR", join(CONFIG_FOLDER, "geoseeq"))
PROFILES_PATH = join(CONFIG_DIR, "profiles.json")


def set_profile(token, endpoint=DEFAULT_ENDPOINT, profile="", overwrite=False):
    """Write a profile to a config file.
    
    Raises KeyError if profile already exists.
    """
    if not exists(PROFILES_PATH):
        makedirs(CONFIG_DIR)
        with open(PROFILES_PATH, "w") as f:
            json.dump({}, f)
    with open(PROFILES_PATH, "r") as f:
        profiles = json.load(f)
    profile = profile or "__default__"
    if profile in profiles and not overwrite:
        raise KeyError(f"Profile {profile} already exists.")
    profiles[profile] = {
        "token": token,
        "endpoint": endpoint,
    }
    with open(PROFILES_PATH, "w") as f:
        json.dump(profiles, f, indent=4)
