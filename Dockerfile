FROM mcr.microsoft.com/devcontainers/go:1.25-bookworm AS builder

ENV GOROOT=/usr/local/go
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:$GOROOT/bin:/usr/local/lib:$PATH
ENV GOSUMDB=off

ARG project
ARG github_username
ARG github_token

##############################################################################
# Copy all the relevant source code in the Docker image, so we can build this.

RUN git config --global \
    url."https://${github_username}:${github_token}@github.com/".insteadOf \
    "https://github.com/"


##############################################################################
# Copy all the relevant source code in the Docker image, so we can build this.

RUN mkdir -p /go/src/github.com/uug-ai/${project}
COPY . /go/src/github.com/uug-ai/${project}

##################
# Build project

RUN cd /go/src/github.com/uug-ai/${project} && \
    go mod download && \
    go build -tags timetzdata,netgo --ldflags '-s -w -extldflags "-static -latomic"' main.go && \
    mkdir -p /${project} && mv main /${project} && \
    rm -rf /go/src/github.com

####################################
# Let's create a /dist folder containing just the files necessary for runtime.
# Later, it will be copied as the / (root) of the output image.

WORKDIR /dist
RUN cp -r /${project} ./

##############################
# Final Stage: Create the small runtime image.

FROM alpine:latest
LABEL org.opencontainers.image.source https://github.com/uug-ai/ingest
LABEL AUTHOR=uug-ai

ARG project

############################
# Protect by non-root user.
    
RUN addgroup -S ${project} && adduser -S ${project} -G ${project}

#################################
# Copy files from previous images

COPY --chown=0:0 --from=builder /dist /

############################
# Move directory to /var/lib

RUN apk update && apk add ca-certificates curl libstdc++ libc6-compat --no-cache && rm -rf /var/cache/apk/*

####################
# Try running project

RUN mv /${project}/* /home/${project}/

###########################
# Grant the necessary root capabilities to the process trying to bind to the privileged port
RUN apk add libcap && setcap 'cap_net_bind_service=+ep' /home/${project}/main

###################
# Run non-root user

USER ${project}

######################################
# By default the app runs on port 80

#EXPOSE 8081

######################################
# Check if the service is still running
# HEALTHCHECK CMD curl --fail http://localhost:8081 || exit 1   

###################################################
# Leeeeettttt'ssss goooooo!!!
# Run the shizzle from the right working directory.

WORKDIR /home/${project}
CMD ["./main"]
