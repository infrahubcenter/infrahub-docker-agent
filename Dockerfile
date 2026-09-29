# Cross-compiles on the build host (pure Go, CGO_ENABLED=0) for each
# --platform target, so multi-arch builds need no emulation.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags="-s -w" -o /out/docker-agent .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/docker-agent /docker-agent
# Deliberately root (no "USER nonroot" -- unlike k8s-agent/Dockerfile):
# this agent talks to the host's Docker daemon over /var/run/docker.sock,
# bind-mounted in at `docker run` time. That socket is owned root:root (or
# root:docker) on the host, and its ownership/GID isn't something this
# image can know or match at build time -- running as a non-root UID would
# make the mount unreadable on most hosts. Two more read-only host mounts
# (/proc, /) exist for host_system_metrics (see hostmetrics.go) -- still
# no --privileged, no extra capabilities, and both are read-only.
ENTRYPOINT ["/docker-agent"]
