ARG BASE=stretch
FROM python:3.6-${BASE} as build

RUN pip install --no-cache-dir pyinstaller
ADD . /k8s-operator/
WORKDIR k8s-operator
RUN pyinstaller -F -n side8-k8s-operator side8/k8s/operator/__init__.py

FROM scratch as run

COPY --from=build k8s-operator/dist/side8-k8s-operator /
