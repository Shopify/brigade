#!/bin/sh
set -e

# export AWS_ACCESS_KEY=`cat ../../config/aws.access`
# export AWS_SECRET_KEY=`cat ../../config/aws.secret`
# export BUCKET_SOURCE="s3://shopify/s"

export AWS_ACCESS_KEY=`cat aws.access`
export AWS_SECRET_KEY=`cat aws.secret`
export BUCKET_SOURCE="s3://shopify/s"

set -x

./brigade-s3-list -bkt=$BUCKET_SOURCE
