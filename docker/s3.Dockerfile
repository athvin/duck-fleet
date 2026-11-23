FROM python:3.12.12-slim-bookworm
RUN pip install --no-cache-dir boto3==1.43.127
ARG TARGETARCH
RUN python -c 'import hashlib, os, tarfile, urllib.request; arch=os.environ["TARGETARCH"]; version="0.1.37"; expected={"amd64":"31e79fe7697c870e60f5b5c9008adbb3ad5af7f9f2ab45a3d68585c5f87238d1","arm64":"a02091205fe4d9fbf0d16a0dac3d46ad7fac7d5266024b5b17e76327a4e88aa8"}[arch]; url=f"https://github.com/rustfs/cli/releases/download/v{version}/rustfs-cli-linux-{arch}-v{version}.tar.gz"; data=urllib.request.urlopen(url).read(); assert hashlib.sha256(data).hexdigest()==expected; open("/tmp/rc.tar.gz","wb").write(data); archive=tarfile.open("/tmp/rc.tar.gz"); archive.extract("rc","/usr/local/bin")'
COPY scripts/object-store.py /app/object-store.py
ENTRYPOINT ["python", "/app/object-store.py"]
