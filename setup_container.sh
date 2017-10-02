#!/bin/bash
set -xe

docker run --privileged -d -it \
-e "AWS_REGION=$AWS_REGION" \
-e "AWS_BUCKET_ROTATION=$AWS_BUCKET_ROTATION" \
-e "AWS_BUCKET_FORBIDDEN=$AWS_BUCKET_FORBIDDEN" \
-e "AWS_BUCKET_UPLOAD=$AWS_BUCKET_UPLOAD" \
-e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
-e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
-v /sys/fs/cgroup:/sys/fs/cgroup \
-v $(pwd)/run_in_docker.sh \
-v $GOPATH:/gopath \
centos:centos7 /usr/sbin/init

DOCKER_CONTAINER_ID=$(docker ps | grep centos | awk '{print $1}')

REPO=/gopath/src/github.com/daniel-cole/GoS3GFSBackup

docker exec -it $DOCKER_CONTAINER_ID /bin/bash -xec "bash -xe $REPO/run_tests_in_container.sh; echo -ne \"------\nTESTS COMPLETE\n\";"

docker ps -a
docker stop $DOCKER_CONTAINER_ID
docker rm -v $DOCKER_CONTAINER_ID

