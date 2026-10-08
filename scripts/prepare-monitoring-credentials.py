"""Extract the Kafka keystore password without printing or committing credentials."""

import argparse
import os
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("directory", nargs="?", default="group15-credentials")
args = parser.parse_args()
directory = Path(args.directory)
properties = {}
for line in (directory / "client-ssl.properties").read_text().splitlines():
    if "=" in line and not line.lstrip().startswith("#"):
        key, value = line.split("=", 1)
        properties[key.strip()] = value.strip()
password = properties.get("ssl.keystore.password")
if not password:
    raise SystemExit("client-ssl.properties is missing ssl.keystore.password")
path = directory / "keystore-password"
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as output:
    output.write(password + "\n")
path.chmod(0o600)
print(f"Prepared password file. Credential owner UID={os.getuid()}, GID={os.getgid()}.")
print("The demo overlay defaults to UID/GID 1000; set TO_CREDENTIAL_UID/GID if different.")
