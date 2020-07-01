#! /bin/bash

set -e

. ./set-env.sh

./gradlew testClasses

docker-compose down -v

docker-compose up --build -d

sleep 10

./gradlew $GRADLE_OPTIONS cleanTest build $GRADLE_TASK_OPTIONS

#Testing old property support

unset RABBITMQ_BROKER_ADDRESSES

export RABBITMQ_URL=$DOCKER_HOST_IP

./gradlew $GRADLE_OPTIONS cleanTest eventuate-messaging-rabbitmq-spring-integration-tests:test --tests "io.eventuate.messaging.rabbitmq.spring.integrationtests.MessagingTest" $GRADLE_TASK_OPTIONS

docker-compose down -v