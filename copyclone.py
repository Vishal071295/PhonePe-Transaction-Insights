repo_url = "https://github.com/PhonePe/pulse.git"
destination = "D:\PHONEPAY\data"
from git import Repo

Repo.clone_from(repo_url, destination)