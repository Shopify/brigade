#!/bin/sh
set -e

# export AWS_ACCESS_KEY=`cat $HOME/key/aws.access`
# export AWS_SECRET_KEY=`cat $HOME/key/aws.secret`
# export BUCKET_SOURCE="s3://shopify-perf/"

export AWS_ACCESS_KEY=`cat .aws/aws.access`
export AWS_SECRET_KEY=`cat .aws/aws.secret`
export BUCKET_SOURCE="s3://shopify/s"

set -x

./brigade-s3-list -bkt=$BUCKET_SOURCE -deduplicate
